from __future__ import annotations

import json
from pathlib import Path
import zipfile

from oplog_fde.validation.audit import audit
from oplog_fde.ingestion.source import chunk_name
from pathlib import PurePosixPath


def event(event_id: str, session_id: str, timestamp_ms: int) -> dict:
    return {
        "schema_version": "1.0.0",
        "event_id": event_id,
        "session_id": session_id,
        "timestamp_ms": timestamp_ms,
        "timestamp_iso": "2026-07-01T00:00:00.000Z",
        "layer": "L2",
        "event_type": "app_switch",
        "source": {"machine_id": "machine-1", "username_hash": "operator-1"},
        "context": {"active_app": {"app_name": "excel"}},
        "correlation": {"sequence_number": 1, "chunk_id": "chunk-1"},
        "payload": {},
    }


def make_tree(root: Path) -> Path:
    chunk = root / "wrapper" / "dataset_a" / "ses_one" / "chunk_one"
    chunk.mkdir(parents=True)
    rows = [event("evt-1", "ses_one", 1), event("evt-2", "wrong-session", 0)]
    (chunk / "events.jsonl").write_text("\n".join(json.dumps(row) for row in rows) + "\n")
    (chunk / "manifest.json").write_text("{}")
    session = chunk.parent
    (session / "gt.jsonl").write_text(json.dumps({"event": "run_config"}) + "\n")
    (session / "gt_manifest.json").write_text("{}")
    return root


def test_audit_discovers_nested_root_and_anomalies(tmp_path: Path) -> None:
    report = audit(make_tree(tmp_path / "input"))
    data = report["datasets"]["dataset_a"]
    assert data["effective_roots"] == ["wrapper/dataset_a"]
    assert data["sessions"] == 1
    assert data["chunks"] == 1
    assert data["event_count"] == 2
    assert data["out_of_order_events"] == 1
    assert data["session_id_mismatches"] == 1


def test_zip_is_audited_without_extraction(tmp_path: Path) -> None:
    tree = make_tree(tmp_path / "tree")
    archive_path = tmp_path / "data.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        for file in tree.rglob("*"):
            if file.is_file():
                archive.write(file, file.relative_to(tree))
    report = audit(archive_path)
    assert report["source_type"] == "zip"
    assert report["datasets"]["dataset_a"]["event_count"] == 2


def test_missing_ground_truth_is_reported(tmp_path: Path) -> None:
    chunk = tmp_path / "dataset_a" / "ses_missing" / "chunk_one"
    chunk.mkdir(parents=True)
    (chunk / "events.jsonl").write_text(json.dumps(event("evt-1", "ses_missing", 0)) + "\n")
    (chunk / "manifest.json").write_text("{}")
    report = audit(tmp_path)
    warning_types = {warning["type"] for warning in report["warnings"]}
    assert "missing_gt.jsonl" in warning_types
    assert "missing_gt_manifest.json" in warning_types


def test_timestamp_reversal_is_reported_without_reordering(tmp_path: Path) -> None:
    report = audit(make_tree(tmp_path / "input"))
    assert report["datasets"]["dataset_a"]["out_of_order_events"] == 1


def test_screenshot_names_are_aggregated(tmp_path: Path) -> None:
    root = make_tree(tmp_path / "input")
    screenshots = root / "wrapper" / "dataset_a" / "ses_one" / "chunk_one" / "screenshots"
    screenshots.mkdir()
    (screenshots / "scr_1.jpg").write_bytes(b"one")
    (screenshots / "scr_2.jpg").write_bytes(b"two")
    report = audit(root)
    counts = report["datasets"]["dataset_a"]["file_counts"]
    assert counts["screenshots/*"] == 2
    assert "scr_1.jpg" not in counts


def test_deepest_chunk_directory_wins_over_drive_wrapper() -> None:
    path = PurePosixPath(
        "dataset_a/ses_one/chunk_1200/chunk_20260701-1200-machine/events.jsonl"
    )
    assert chunk_name(path) == "chunk_20260701-1200-machine"