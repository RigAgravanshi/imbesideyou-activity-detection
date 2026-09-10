from __future__ import annotations

from types import SimpleNamespace

from oplog_fde.ingestion.sessionize import EventRecord
from oplog_fde.validation.reconstruction_v2 import gt_report, reversal_report


def _event(timestamp: int, sequence: int) -> EventRecord:
    return EventRecord(
        data={"timestamp_ms": timestamp},
        dataset="dataset_a",
        session_id="session",
        chunk_id="chunk",
        source_path="events.jsonl",
        source_line=sequence,
        sequence_number=sequence,
        is_recorder_activity=False,
    )


def test_reversal_check_resets_between_sessions() -> None:
    sessions = [
        SimpleNamespace(events=[_event(1000, 1), _event(900, 2)]),
        SimpleNamespace(events=[_event(100, 1), _event(200, 2)]),
    ]

    result = reversal_report(sessions)

    assert result["count"] == 1
    assert result["magnitude_ms"]["max"] == 100


def test_incomplete_manifest_interval_is_not_scored_as_failure() -> None:
    executions = [{
        "session_id": "session",
        "process_code": "A",
        "case_id": "CASE-1",
        "start": "2026-07-01T00:00:00Z",
        "end": None,
    }]

    result = gt_report(executions, [])

    assert result["manifest_executions"] == 1
    assert result["complete_manifest_executions_evaluated"] == 0
    assert result["incomplete_manifest_executions"] == 1
    assert result["match_rate_among_complete_executions"] is None