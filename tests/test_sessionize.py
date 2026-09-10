from __future__ import annotations

import json
from pathlib import Path

from oplog_fde.ingestion.sessionize import (
    ground_truth_fragments,
    profile,
    reconstruct_sessions,
)


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _event(event_id: str, sequence: int, timestamp: int, event_type: str = "mouse_click") -> dict:
    return {
        "event_id": event_id,
        "session_id": "ses_one",
        "timestamp_ms": timestamp,
        "timestamp_iso": "2026-07-01T00:00:00Z",
        "layer": "SYSTEM" if event_type == "session_start" else "L2",
        "event_type": event_type,
        "correlation": {"sequence_number": sequence},
    }


def test_reconstructs_chunks_without_global_timestamp_sort(tmp_path: Path) -> None:
    first = tmp_path / "dataset_a" / "ses_one" / "chunk_01"
    second = tmp_path / "dataset_a" / "ses_one" / "chunk_02"
    _write_json(first / "manifest.json", {"time_range": {"start_ms": 1000}})
    _write_json(second / "manifest.json", {"time_range": {"start_ms": 2000}})
    _write_jsonl(
        first / "events.jsonl",
        [_event("later-in-first", 2, 1300), _event("first", 1, 1000, "session_start")],
    )
    # Timestamp 1290 reverses slightly, but chunk order must remain authoritative.
    _write_jsonl(second / "events.jsonl", [_event("second-chunk", 1, 1290)])

    session = reconstruct_sessions(str(tmp_path))[0]

    assert [event.data["event_id"] for event in session.events] == [
        "first",
        "later-in-first",
        "second-chunk",
    ]
    assert session.timestamp_reversals == 1
    assert [event.data["event_id"] for event in session.activity_events] == [
        "later-in-first",
        "second-chunk",
    ]


def test_deduplicates_overlap_by_event_id_and_reports_conflicts(tmp_path: Path) -> None:
    first = tmp_path / "dataset_b" / "ses_one" / "chunk_01"
    second = tmp_path / "dataset_b" / "ses_one" / "chunk_02"
    _write_json(first / "manifest.json", {"time_range": {"start_ms": 1000}})
    _write_json(second / "manifest.json", {"time_range": {"start_ms": 2000}})
    original = _event("overlap", 1, 1000)
    _write_jsonl(first / "events.jsonl", [original])
    _write_jsonl(second / "events.jsonl", [original, _event("overlap", 2, 2001)])

    session = reconstruct_sessions(str(tmp_path))[0]

    assert len(session.events) == 1
    assert session.duplicate_events_removed == 2
    assert session.conflicting_event_ids == 1


def test_prefers_deepest_merged_drive_copy(tmp_path: Path) -> None:
    shallow = tmp_path / "dataset_b" / "ses_one" / "chunk_01" / "events.jsonl"
    deep = (
        tmp_path
        / "wrapper"
        / "dataset_b"
        / "ses_one"
        / "copied"
        / "chunk_01"
        / "events.jsonl"
    )
    _write_jsonl(shallow, [_event("shallow", 1, 1000)])
    _write_jsonl(deep, [_event("deep", 1, 1000)])

    session = reconstruct_sessions(str(tmp_path))[0]

    assert [event.data["event_id"] for event in session.events] == ["deep"]


def test_ground_truth_suspension_becomes_two_fragments_with_same_identity() -> None:
    rows = [
        {
            "event": "process_started",
            "ts_utc": "2026-07-01T00:00:00+00:00",
            "process_code": "A",
            "process_name": "Process A",
            "case_id": "CASE-1",
        },
        {  # documented duplicate start must not create a zero-length fragment
            "event": "process_started",
            "ts_utc": "2026-07-01T00:00:01+00:00",
            "process_code": "A",
            "case_id": "CASE-1",
        },
        {
            "event": "process_suspended",
            "ts_utc": "2026-07-01T00:00:10+00:00",
            "split_id": "split-1",
        },
        {
            "event": "process_started",
            "ts_utc": "2026-07-01T00:00:10+00:00",
            "process_code": "B",
            "case_id": "CASE-2",
        },
        {
            "event": "process_switched_out",
            "ts_utc": "2026-07-01T00:00:20+00:00",
        },
        {
            "event": "process_resumed",
            "ts_utc": "2026-07-01T00:00:20+00:00",
            "process_code": "A",
            "case_id": "CASE-1",
            "split_id": "split-1",
            "phase": 2,
        },
        {"event": "session_ended", "ts_utc": "2026-07-01T00:00:30+00:00"},
    ]

    fragments = ground_truth_fragments("ses_one", rows)

    assert [fragment.process_code for fragment in fragments] == ["A", "B", "A"]
    assert fragments[0].case_id == fragments[2].case_id == "CASE-1"
    assert fragments[0].end == fragments[1].start
    assert fragments[1].end == fragments[2].start


def test_profile_separates_recorder_and_worker_activity(tmp_path: Path) -> None:
    chunk = tmp_path / "dataset_b" / "ses_one" / "chunk_01"
    _write_jsonl(
        chunk / "events.jsonl",
        [_event("start", 1, 1000, "session_start"), _event("work", 2, 1001)],
    )

    result = profile(reconstruct_sessions(str(tmp_path)))

    assert result["events"] == 2
    assert result["recorder_events"] == 1
    assert result["activity_events"] == 1