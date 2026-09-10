from __future__ import annotations

from oplog_fde.features.boundary_signals import transition_signals
from oplog_fde.ingestion.sessionize import EventRecord


def _record(event_id: str, timestamp: int, app: str, title: str, event_type: str = "mouse_click") -> EventRecord:
    data = {
        "event_id": event_id,
        "timestamp_ms": timestamp,
        "event_type": event_type,
        "context": {"active_app": {"app_name": app, "window_title": title}},
        "payload": {},
    }
    return EventRecord(data, "dataset_a", "ses_one", "chunk_one", "events.jsonl", 1, 1, False)


def test_transition_signals_detect_transferable_context_changes() -> None:
    previous = _record("one", 1_000, "Excel", "Claim EXP-1234")
    current = _record("two", 7_000, "Chrome", "Claim EXP-5678")

    signals = transition_signals(previous, current)

    assert signals["gap_ge_5s"]
    assert signals["app_changed"]
    assert signals["page_changed"]
    assert signals["identifier_changed"]


def test_app_switch_alone_does_not_imply_a_boundary() -> None:
    previous = _record("one", 1_000, "Excel", "Claim EXP-1234")
    current = _record("two", 1_100, "Chrome", "Claim EXP-1234")

    signals = transition_signals(previous, current)

    assert signals["app_changed"]
    assert not signals["identifier_changed"]
    assert not signals["gap_ge_5s"]
