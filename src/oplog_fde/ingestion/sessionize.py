"""Reconstruct logical sessions and Dataset A ground-truth fragments.

Ordering policy:
* chunks are ordered by manifest start, falling back to their earliest event timestamp;
* events are ordered by correlation.sequence_number within each chunk;
* timestamps are retained for elapsed-time calculations but are never used to globally
  reorder a session.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
from pathlib import PurePosixPath
from typing import Any, Iterable, Iterator

from oplog_fde.ingestion.source import (
    DataSource,
    Member,
    chunk_name,
    dataset_kind,
    session_name,
)


RECORDER_EVENT_TYPES = {
    "session_start",
    "session_end",
    "extension_connected",
    "extension_disconnected",
    "upload_started",
    "upload_completed",
    "upload_failed",
}


@dataclass(frozen=True)
class EventRecord:
    data: dict[str, Any]
    dataset: str
    session_id: str
    chunk_id: str
    source_path: str
    source_line: int
    sequence_number: int | None
    is_recorder_activity: bool


@dataclass(frozen=True)
class GroundTruthFragment:
    session_id: str
    process_code: str
    process_name: str | None
    case_id: str | None
    split_id: str | None
    phase: str | int | None
    start: str
    end: str
    end_reason: str


@dataclass
class ReconstructedSession:
    dataset: str
    session_id: str
    chunk_ids: list[str]
    events: list[EventRecord]
    ground_truth: list[GroundTruthFragment] = field(default_factory=list)
    duplicate_events_removed: int = 0
    conflicting_event_ids: int = 0
    timestamp_reversals: int = 0

    @property
    def activity_events(self) -> list[EventRecord]:
        return [event for event in self.events if not event.is_recorder_activity]


@dataclass
class _LoadedChunk:
    dataset: str
    session_id: str
    chunk_id: str
    member: Member
    start_ms: int
    events: list[EventRecord]


def _json_lines(source: DataSource, member: Member) -> Iterator[tuple[int, dict[str, Any]]]:
    with source.open_text(member) as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON at {member.path}:{line_number}: {exc}") from exc
            if not isinstance(value, dict):
                raise ValueError(f"Expected object at {member.path}:{line_number}")
            yield line_number, value


def _sequence(event: dict[str, Any]) -> int | None:
    correlation = event.get("correlation")
    value = correlation.get("sequence_number") if isinstance(correlation, dict) else None
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _is_recorder_event(event: dict[str, Any]) -> bool:
    return event.get("layer") == "SYSTEM" or event.get("event_type") in RECORDER_EVENT_TYPES


def _manifest_start(source: DataSource, member: Member | None) -> int | None:
    if member is None:
        return None
    try:
        with source.open_text(member) as handle:
            value = json.load(handle)
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict):
        return None
    time_range = value.get("time_range")
    if isinstance(time_range, dict):
        start = time_range.get("start_ms")
        if isinstance(start, (int, float)) and not isinstance(start, bool):
            return int(start)
    return None


def _real_event_members(members: Iterable[Member]) -> list[Member]:
    """Return actual event logs, preferring the deepest path for merged duplicates."""
    candidates: dict[tuple[str, str, str], Member] = {}
    for member in members:
        if member.path.name != "events.jsonl":
            continue
        kind = dataset_kind(member.path)
        session = session_name(member.path)
        chunk = chunk_name(member.path)
        if not (kind and session and chunk):
            continue
        key = (kind, session, chunk)
        previous = candidates.get(key)
        if previous is None or len(member.path.parts) > len(previous.path.parts):
            candidates[key] = member
    return sorted(candidates.values(), key=lambda item: item.path.as_posix())


def _load_chunk(
    source: DataSource,
    event_member: Member,
    manifest_by_parent: dict[PurePosixPath, Member],
) -> _LoadedChunk:
    kind = dataset_kind(event_member.path)
    session = session_name(event_member.path)
    chunk = chunk_name(event_member.path)
    assert kind and session and chunk
    records: list[EventRecord] = []
    timestamp_candidates: list[int] = []
    for line_number, event in _json_lines(source, event_member):
        timestamp = event.get("timestamp_ms")
        if isinstance(timestamp, (int, float)) and not isinstance(timestamp, bool):
            timestamp_candidates.append(int(timestamp))
        records.append(
            EventRecord(
                data=event,
                dataset=kind,
                session_id=session,
                chunk_id=chunk,
                source_path=event_member.path.as_posix(),
                source_line=line_number,
                sequence_number=_sequence(event),
                is_recorder_activity=_is_recorder_event(event),
            )
        )
    # Missing sequence values retain their file position after sequenced events.
    records.sort(
        key=lambda item: (
            item.sequence_number is None,
            item.sequence_number if item.sequence_number is not None else item.source_line,
            item.source_line,
        )
    )
    manifest_start = _manifest_start(source, manifest_by_parent.get(event_member.path.parent))
    fallback_start = min(timestamp_candidates) if timestamp_candidates else 2**63 - 1
    return _LoadedChunk(kind, session, chunk, event_member, manifest_start or fallback_start, records)


def _event_digest(event: dict[str, Any]) -> str:
    encoded = json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.blake2b(encoded, digest_size=16).hexdigest()


def _deduplicate(events: Iterable[EventRecord]) -> tuple[list[EventRecord], int, int]:
    retained: list[EventRecord] = []
    ids: dict[str, str] = {}
    digests: set[str] = set()
    removed = conflicts = 0
    for record in events:
        digest = _event_digest(record.data)
        event_id = record.data.get("event_id")
        if isinstance(event_id, str) and event_id:
            previous_digest = ids.get(event_id)
            if previous_digest is not None:
                removed += 1
                conflicts += int(previous_digest != digest)
                continue
            ids[event_id] = digest
        elif digest in digests:
            removed += 1
            continue
        digests.add(digest)
        retained.append(record)
    return retained, removed, conflicts


def _timestamp_reversals(events: Iterable[EventRecord]) -> int:
    reversals = 0
    previous: int | float | None = None
    for record in events:
        current = record.data.get("timestamp_ms")
        if not isinstance(current, (int, float)) or isinstance(current, bool):
            continue
        if previous is not None and current < previous:
            reversals += 1
        previous = current
    return reversals


def reconstruct_sessions(path: str) -> list[ReconstructedSession]:
    source = DataSource(path)
    members = source.members()
    manifest_by_parent = {
        member.path.parent: member for member in members if member.path.name == "manifest.json"
    }
    grouped: dict[tuple[str, str], list[_LoadedChunk]] = defaultdict(list)
    for event_member in _real_event_members(members):
        chunk = _load_chunk(source, event_member, manifest_by_parent)
        grouped[(chunk.dataset, chunk.session_id)].append(chunk)

    gt_by_session = _ground_truth_by_session(source, members)
    sessions: list[ReconstructedSession] = []
    for (kind, session_id), chunks in sorted(grouped.items()):
        chunks.sort(key=lambda item: (item.start_ms, item.chunk_id, item.member.path.as_posix()))
        combined = [event for chunk in chunks for event in chunk.events]
        events, removed, conflicts = _deduplicate(combined)
        sessions.append(
            ReconstructedSession(
                dataset=kind,
                session_id=session_id,
                chunk_ids=[chunk.chunk_id for chunk in chunks],
                events=events,
                ground_truth=gt_by_session.get(session_id, []),
                duplicate_events_removed=removed,
                conflicting_event_ids=conflicts,
                timestamp_reversals=_timestamp_reversals(events),
            )
        )
    return sessions


def _iso(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _process_fields(value: Any, fallback: dict[str, Any]) -> dict[str, Any]:
    source = value if isinstance(value, dict) else {}
    code = source.get("process_code") or source.get("code")
    if not code and isinstance(value, str):
        code = value
    return {
        "process_code": code or fallback.get("process_code") or fallback.get("code"),
        "process_name": source.get("process_name") or source.get("name") or fallback.get("process_name"),
        "case_id": source.get("case_id") or fallback.get("case_id"),
        "split_id": source.get("split_id") or fallback.get("split_id"),
        "phase": source.get("phase") or fallback.get("phase"),
    }


def ground_truth_fragments(session_id: str, rows: Iterable[dict[str, Any]]) -> list[GroundTruthFragment]:
    """Convert GT transitions into non-overlapping active-process fragments."""
    active: dict[str, Any] | None = None
    suspended: dict[str, dict[str, Any]] = {}
    fragments: list[GroundTruthFragment] = []

    def close(end: str, reason: str) -> None:
        nonlocal active
        if active is None or end < active["start"]:
            return
        fragments.append(
            GroundTruthFragment(
                session_id=session_id,
                process_code=str(active["process_code"]),
                process_name=active.get("process_name"),
                case_id=active.get("case_id"),
                split_id=active.get("split_id"),
                phase=active.get("phase"),
                start=active["start"],
                end=end,
                end_reason=reason,
            )
        )
        active = None

    for row in rows:
        event_type = row.get("event") or row.get("event_type")
        timestamp = _iso(row.get("ts_utc") or row.get("timestamp_iso"))
        if not timestamp:
            continue
        if event_type == "process_started":
            fields = _process_fields(row, row)
            if not fields["process_code"]:
                continue
            same_execution = active and all(
                active.get(key) == fields.get(key) for key in ("process_code", "case_id")
            )
            if same_execution:  # documented consecutive duplicate
                continue
            close(timestamp, "implicit_switch")
            active = {**fields, "start": timestamp}
        elif event_type in {"process_switched_out", "process_suspended"}:
            if active is not None:
                split_id = row.get("split_id") or active.get("split_id")
                if event_type == "process_suspended" and split_id:
                    active["split_id"] = split_id
                    suspended[str(split_id)] = dict(active)
                close(timestamp, event_type.removeprefix("process_"))
        elif event_type == "process_resumed":
            split_id = row.get("split_id")
            fields = dict(suspended.get(str(split_id), {})) if split_id else {}
            fields.update({k: v for k, v in _process_fields(row, row).items() if v is not None})
            if fields.get("process_code"):
                close(timestamp, "implicit_switch")
                active = {**fields, "start": timestamp}
        elif event_type == "session_ended":
            close(timestamp, "session_end")
    return fragments


def _ground_truth_by_session(
    source: DataSource, members: Iterable[Member]
) -> dict[str, list[GroundTruthFragment]]:
    result: dict[str, list[GroundTruthFragment]] = {}
    for member in members:
        if member.path.name != "gt.jsonl":
            continue
        session = session_name(member.path)
        if session:
            result[session] = ground_truth_fragments(
                session, (row for _, row in _json_lines(source, member))
            )
    return result


def profile(sessions: Iterable[ReconstructedSession]) -> dict[str, Any]:
    session_list = list(sessions)
    by_dataset = Counter(session.dataset for session in session_list)
    return {
        "sessions": len(session_list),
        "sessions_by_dataset": dict(sorted(by_dataset.items())),
        "chunks": sum(len(session.chunk_ids) for session in session_list),
        "events": sum(len(session.events) for session in session_list),
        "activity_events": sum(len(session.activity_events) for session in session_list),
        "recorder_events": sum(
            len(session.events) - len(session.activity_events) for session in session_list
        ),
        "duplicate_events_removed": sum(
            session.duplicate_events_removed for session in session_list
        ),
        "conflicting_event_ids": sum(session.conflicting_event_ids for session in session_list),
        "timestamp_reversals": sum(session.timestamp_reversals for session in session_list),
        "ground_truth_fragments": sum(len(session.ground_truth) for session in session_list),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Reconstruct operation-log sessions")
    parser.add_argument("data_parent", help="Directory or ZIP containing dataset_a/dataset_b")
    parser.add_argument("--output", help="Optional JSON profile path")
    args = parser.parse_args()
    result = profile(reconstruct_sessions(args.data_parent))
    rendered = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        from pathlib import Path

        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)


if __name__ == "__main__":
    main()