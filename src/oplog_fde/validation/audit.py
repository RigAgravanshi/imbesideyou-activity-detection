from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime
import hashlib
import json
from pathlib import Path, PurePosixPath
from typing import Any

from oplog_fde.ingestion.source import (
    DataSource,
    Member,
    chunk_name,
    dataset_kind,
    session_name,
)

EXPECTED_EVENT_KEYS = {
    "schema_version",
    "event_id",
    "session_id",
    "timestamp_ms",
    "timestamp_iso",
    "layer",
    "event_type",
    "source",
    "context",
    "correlation",
    "payload",
}


def _parse_iso(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _safe_json(source: DataSource, member: Member, errors: list[dict[str, Any]]) -> Any:
    try:
        with source.open_text(member) as handle:
            return json.load(handle)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        errors.append({"path": str(member.path), "error": str(exc)})
        return None


def _member_key(member: Member) -> tuple[str | None, str | None, str]:
    return dataset_kind(member.path), session_name(member.path), member.path.name


def audit(path: str | Path) -> dict[str, Any]:
    source = DataSource(path)
    members = source.members()
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    chunk_ranges: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)

    datasets: dict[str, dict[str, Any]] = {
        name: {
            "effective_roots": set(),
            "sessions": set(),
            "chunks": set(),
            "file_counts": Counter(),
            "event_count": 0,
            "gt_record_count": 0,
            "event_types": Counter(),
            "layers": Counter(),
            "applications": Counter(),
            "operators": Counter(),
            "machines": Counter(),
            "events_with_extracted_text": 0,
            "screenshot_references": 0,
            "timestamp_min": None,
            "timestamp_max": None,
            "out_of_order_events": 0,
            "duplicate_event_ids": 0,
            "duplicate_event_content": 0,
            "session_id_mismatches": 0,
            "timestamp_ms_iso_mismatches": 0,
            "missing_expected_event_keys": Counter(),
        }
        for name in ("dataset_a", "dataset_b")
    }

    path_counts = Counter(str(m.path) for m in members)
    duplicate_member_paths = sorted(p for p, count in path_counts.items() if count > 1)
    logical_locations: dict[tuple[str | None, str | None, str | None, str], list[str]] = defaultdict(list)

    for member in members:
        kind = dataset_kind(member.path)
        session = session_name(member.path)
        chunk = chunk_name(member.path)
        logical_locations[(kind, session, chunk, member.path.name)].append(str(member.path))
        if kind is None:
            continue
        info = datasets[kind]
        parts_lower = [p.lower() for p in member.path.parts]
        index = parts_lower.index(kind)
        info["effective_roots"].add(PurePosixPath(*member.path.parts[: index + 1]).as_posix())
        if session:
            info["sessions"].add(session)
        if session and chunk:
            info["chunks"].add(f"{session}/{chunk}")
        file_key = (
            "screenshots/*"
            if "screenshots" in [part.lower() for part in member.path.parts]
            else member.path.name
        )
        info["file_counts"][file_key] += 1

    duplicate_logical_files = [
        {"logical_key": list(key), "paths": locations}
        for key, locations in logical_locations.items()
        if key[0] and len(locations) > 1
    ]

    for member in members:
        kind = dataset_kind(member.path)
        if kind is None:
            continue
        info = datasets[kind]
        expected_session = session_name(member.path)
        if member.path.name == "manifest.json":
            obj = _safe_json(source, member, errors)
            if isinstance(obj, dict):
                for candidate in (obj.get("session_id"), (obj.get("session") or {}).get("id") if isinstance(obj.get("session"), dict) else None):
                    if candidate and expected_session and candidate != expected_session:
                        warnings.append({"type": "manifest_session_id_mismatch", "path": str(member.path), "value": candidate, "directory": expected_session})
                time_range = obj.get("time_range") or {}
                if expected_session and isinstance(time_range, dict):
                    start = time_range.get("start_ms")
                    end = time_range.get("end_ms")
                    if isinstance(start, (int, float)) and isinstance(end, (int, float)):
                        chunk_ranges[(kind, expected_session)].append(
                            {"chunk": chunk_name(member.path), "start_ms": start, "end_ms": end, "path": str(member.path)}
                        )
                statistics = obj.get("statistics") or {}
                files = obj.get("files") or {}
                declared_events = statistics.get("total_events") if isinstance(statistics, dict) else None
                event_file = next(
                    (m for m in members if m.path.parent == member.path.parent and m.path.name == "events.jsonl"),
                    None,
                )
                if event_file and isinstance(declared_events, int):
                    with source.open_text(event_file) as event_handle:
                        observed_lines = sum(1 for line in event_handle if line.strip())
                    if observed_lines != declared_events:
                        warnings.append({"type": "manifest_event_count_mismatch", "path": str(member.path), "declared": declared_events, "observed": observed_lines})
                declared_event_file = files.get("event_log") if isinstance(files, dict) else None
                if event_file and isinstance(declared_event_file, dict) and isinstance(declared_event_file.get("size_bytes"), int) and declared_event_file["size_bytes"] != event_file.size:
                    warnings.append({"type": "manifest_event_size_mismatch", "path": str(member.path), "declared": declared_event_file["size_bytes"], "observed": event_file.size})
        elif member.path.name == "gt_manifest.json":
            _safe_json(source, member, errors)
        elif member.path.name == "gt.jsonl":
            with source.open_text(member) as handle:
                for line_number, line in enumerate(handle, 1):
                    if not line.strip():
                        continue
                    try:
                        json.loads(line)
                        info["gt_record_count"] += 1
                    except json.JSONDecodeError as exc:
                        errors.append({"path": str(member.path), "line": line_number, "error": str(exc)})
        elif member.path.name == "events.jsonl":
            seen_ids: set[str] = set()
            seen_content: set[str] = set()
            previous_ms: int | None = None
            with source.open_text(member) as handle:
                for line_number, line in enumerate(handle, 1):
                    if not line.strip():
                        continue
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError as exc:
                        errors.append({"path": str(member.path), "line": line_number, "error": str(exc)})
                        continue
                    if not isinstance(event, dict):
                        errors.append({"path": str(member.path), "line": line_number, "error": "event is not an object"})
                        continue
                    info["event_count"] += 1
                    missing = EXPECTED_EVENT_KEYS - event.keys()
                    info["missing_expected_event_keys"].update(missing)
                    event_id = event.get("event_id")
                    if isinstance(event_id, str):
                        if event_id in seen_ids:
                            info["duplicate_event_ids"] += 1
                        seen_ids.add(event_id)
                    canonical = json.dumps(event, ensure_ascii=False, sort_keys=True).encode()
                    digest = hashlib.blake2b(canonical, digest_size=16).hexdigest()
                    if digest in seen_content:
                        info["duplicate_event_content"] += 1
                    seen_content.add(digest)
                    actual_session = event.get("session_id")
                    if expected_session and actual_session != expected_session:
                        info["session_id_mismatches"] += 1
                    timestamp_ms = event.get("timestamp_ms")
                    if isinstance(timestamp_ms, (int, float)):
                        if previous_ms is not None and timestamp_ms < previous_ms:
                            info["out_of_order_events"] += 1
                        previous_ms = int(timestamp_ms)
                        current_min = info["timestamp_min"]
                        current_max = info["timestamp_max"]
                        info["timestamp_min"] = int(timestamp_ms) if current_min is None else min(current_min, int(timestamp_ms))
                        info["timestamp_max"] = int(timestamp_ms) if current_max is None else max(current_max, int(timestamp_ms))
                        parsed = _parse_iso(event.get("timestamp_iso"))
                        if parsed and abs(parsed.timestamp() * 1000 - timestamp_ms) > 1:
                            info["timestamp_ms_iso_mismatches"] += 1
                    info["event_types"][str(event.get("event_type"))] += 1
                    info["layers"][str(event.get("layer"))] += 1
                    context = event.get("context") or {}
                    if isinstance(context, dict):
                        app = context.get("active_app") or {}
                        if isinstance(app, dict):
                            app_name = app.get("app_name") or app.get("process_name")
                            if app_name:
                                info["applications"][str(app_name)] += 1
                        if context.get("extracted_text"):
                            info["events_with_extracted_text"] += 1
                    source_obj = event.get("source") or {}
                    if isinstance(source_obj, dict):
                        if source_obj.get("username_hash"):
                            info["operators"][str(source_obj["username_hash"])] += 1
                        if source_obj.get("machine_id"):
                            info["machines"][str(source_obj["machine_id"])] += 1
                    payload = event.get("payload") or {}
                    if isinstance(payload, dict) and event.get("event_type") == "screenshot_smart":
                        if payload.get("file_reference"):
                            info["screenshot_references"] += 1

    # Per-session/chunk structural expectations and chunk interval overlaps.
    member_paths = {str(m.path) for m in members}
    for kind, info in datasets.items():
        for session in sorted(info["sessions"]):
            session_members = [m for m in members if dataset_kind(m.path) == kind and session_name(m.path) == session]
            names = {m.path.name for m in session_members}
            if kind == "dataset_a":
                for required in ("gt.jsonl", "gt_manifest.json"):
                    if required not in names:
                        warnings.append({"type": f"missing_{required}", "dataset": kind, "session": session})
            elif "gt.jsonl" in names or "gt_manifest.json" in names:
                warnings.append({"type": "unexpected_ground_truth", "dataset": kind, "session": session})
            chunks = sorted({chunk_name(m.path) for m in session_members if chunk_name(m.path)})
            for chunk in chunks:
                chunk_members = [m for m in session_members if chunk_name(m.path) == chunk]
                chunk_names = {m.path.name for m in chunk_members}
                for required in ("events.jsonl", "manifest.json"):
                    if required not in chunk_names:
                        warnings.append({"type": f"missing_{required}", "dataset": kind, "session": session, "chunk": chunk})
            ranges = sorted(chunk_ranges.get((kind, session), []), key=lambda row: row["start_ms"])
            for previous, current in zip(ranges, ranges[1:]):
                if current["start_ms"] <= previous["end_ms"]:
                    warnings.append({
                        "type": "overlapping_chunks",
                        "dataset": kind,
                        "session": session,
                        "previous_chunk": previous["chunk"],
                        "current_chunk": current["chunk"],
                        "overlap_ms": previous["end_ms"] - current["start_ms"],
                    })

    serializable: dict[str, Any] = {}
    for kind, info in datasets.items():
        timestamp_min = info["timestamp_min"]
        timestamp_max = info["timestamp_max"]
        duration_seconds = None if timestamp_min is None or timestamp_max is None else (timestamp_max - timestamp_min) / 1000
        serializable[kind] = {
            **info,
            "effective_roots": sorted(info["effective_roots"]),
            "sessions": len(info["sessions"]),
            "session_ids": sorted(info["sessions"]),
            "chunks": len(info["chunks"]),
            "file_counts": dict(info["file_counts"].most_common()),
            "event_types": dict(info["event_types"].most_common()),
            "layers": dict(info["layers"].most_common()),
            "applications": dict(info["applications"].most_common()),
            "operators": len(info["operators"]),
            "operator_ids": sorted(info["operators"]),
            "machines": len(info["machines"]),
            "machine_ids": sorted(info["machines"]),
            "duration_seconds": duration_seconds,
            "missing_expected_event_keys": dict(info["missing_expected_event_keys"].most_common()),
        }
        serializable[kind].pop("timestamp_min")
        serializable[kind].pop("timestamp_max")

    return {
        "audit_version": "0.1.0",
        "source": str(source.path),
        "source_type": "zip" if source.is_zip else "directory",
        "member_count": len(members),
        "duplicate_member_paths": duplicate_member_paths,
        "duplicate_logical_files": duplicate_logical_files,
        "datasets": serializable,
        "errors": errors,
        "warnings": warnings,
        "unclassified_files": sum(1 for member in members if dataset_kind(member.path) is None),
        "member_paths_indexed": len(member_paths),
    }


def render_markdown(report: dict[str, Any]) -> str:
    lines = ["# Data Audit", "", f"Source type: `{report['source_type']}`", ""]
    lines += ["| Dataset | Roots | Sessions | Chunks | Events | GT records |", "|---|---:|---:|---:|---:|---:|"]
    for kind, data in report["datasets"].items():
        lines.append(f"| {kind} | {len(data['effective_roots'])} | {data['sessions']} | {data['chunks']} | {data['event_count']} | {data['gt_record_count']} |")
    lines += ["", "## Integrity signals", "", f"- Parse/read errors: **{len(report['errors'])}**", f"- Structural warnings: **{len(report['warnings'])}**", f"- Duplicate archive member paths: **{len(report['duplicate_member_paths'])}**", f"- Duplicate logical files: **{len(report['duplicate_logical_files'])}**"]
    for kind, data in report["datasets"].items():
        lines += ["", f"## {kind}", "", f"- Effective roots: {', '.join(data['effective_roots']) or 'none'}", f"- Operators / machines: {data['operators']} / {data['machines']}", f"- Out-of-order events: {data['out_of_order_events']}", f"- Duplicate event IDs / content: {data['duplicate_event_ids']} / {data['duplicate_event_content']}", f"- Session-ID mismatches: {data['session_id_mismatches']}", f"- Events with extracted text: {data['events_with_extracted_text']}", f"- Screenshot references: {data['screenshot_references']}", f"- Top event types: {dict(list(data['event_types'].items())[:10])}", f"- Top applications: {dict(list(data['applications'].items())[:10])}"]
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", help="Extracted dataset root or ZIP archive")
    parser.add_argument("--output", type=Path, default=Path("outputs/audit/data_audit.json"))
    args = parser.parse_args()
    report = audit(args.source)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown = args.output.with_suffix(".md")
    markdown.write_text(render_markdown(report), encoding="utf-8")
    print(f"Wrote {args.output} and {markdown}")
    if report["errors"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()