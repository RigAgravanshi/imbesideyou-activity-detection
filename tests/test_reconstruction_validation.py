from __future__ import annotations

from oplog_fde.ingestion.sessionize import EventRecord, GroundTruthFragment
from oplog_fde.validation.reconstruction import (
    ground_truth_reconciliation,
    reversal_diagnostics,
)


def _record(timestamp_ms: int, sequence: int) -> EventRecord:
    return EventRecord(
        data={"timestamp_ms": timestamp_ms},
        dataset="dataset_a",
        session_id="ses_one",
        chunk_id="chunk_one",
        source_path="events.jsonl",
        source_line=sequence,
        sequence_number=sequence,
        is_recorder_activity=False,
    )


def test_reversal_diagnostics_reports_magnitude_not_only_count() -> None:
    result = reversal_diagnostics([
        _record(1000, 1),
        _record(995, 2),    # 5 ms
        _record(1200, 3),
        _record(1050, 4),   # 150 ms
        _record(4000, 5),
        _record(2000, 6),   # 2000 ms
    ])

    assert result["count"] == 3
    assert result["buckets"] == {
        "le_10_ms": 1,
        "11_100_ms": 0,
        "101_1000_ms": 1,
        "gt_1000_ms": 1,
    }
    assert result["magnitude_ms"]["max"] == 2000


def test_manifest_execution_can_be_covered_by_resumed_fragments() -> None:
    execution = {
        "session_id": "ses_one",
        "process_code": "A",
        "case_id": "CASE-1",
        "start": "2026-07-01T00:00:00Z",
        "end": "2026-07-01T00:00:20Z",
    }
    fragments = [
        GroundTruthFragment(
            "ses_one", "A", None, "CASE-1", "split-1", 1,
            "2026-07-01T00:00:00Z", "2026-07-01T00:00:10Z", "suspended",
        ),
        GroundTruthFragment(
            "ses_one", "A", None, "CASE-1", "split-1", 2,
            "2026-07-01T00:00:10Z", "2026-07-01T00:00:20Z", "session_end",
        ),
    ]

    result = ground_truth_reconciliation([execution], fragments)

    assert result["match_rate"] == 1.0
    assert result["median_duration_coverage"] == 1.0
