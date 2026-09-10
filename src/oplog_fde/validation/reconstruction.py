"""Validate reconstructed ordering and Dataset A ground-truth coverage."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
import math
from pathlib import Path
from typing import Any, Iterable

from oplog_fde.ingestion.sessionize import (
    EventRecord,
    GroundTruthFragment,
    reconstruct_sessions,
)
from oplog_fde.ingestion.source import DataSource, Member, session_name


def _percentile(values: list[int], percentile: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return ordered[index]


def reversal_diagnostics(events: Iterable[EventRecord]) -> dict[str, Any]:
    """Measure backward timestamp jumps without changing event order."""
    magnitudes: list[int] = []
    previous: int | float | None = None
    for record in events:
        current = record.data.get("timestamp_ms")
        if not isinstance(current, (int, float)) or isinstance(current, bool):
            continue
        if previous is not None and current < previous:
            magnitudes.append(int(previous - current))
        previous = current
    buckets = Counter()
    for magnitude in magnitudes:
        if magnitude <= 10:
            buckets["le_10_ms"] += 1
        elif magnitude <= 100:
            buckets["11_100_ms"] += 1
        elif magnitude <= 1000:
            buckets["101_1000_ms"] += 1
        else:
            buckets["gt_1000_ms"] += 1
    return {
        "count": len(magnitudes),
        "magnitude_ms": {
            "p50": _percentile(magnitudes, 0.50),
            "p95": _percentile(magnitudes, 0.95),
            "p99": _percentile(magnitudes, 0.99),
            "max": max(magnitudes, default=None),
        },
        "buckets": {name: buckets[name] for name in (
            "le_10_ms", "11_100_ms", "101_1000_ms", "gt_1000_ms"
        )},
    }


def _parse_time(value: Any) -> float | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _read_json(source: DataSource, member: Member) -> dict[str, Any] | None:
    try:
        with source.open_text(member) as handle:
            value = json.load(handle)
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _manifest_executions(source: DataSource) -> list[dict[str, Any]]:
    executions: list[dict[str, Any]] = []
    for member in source.members():
        if member.path.name != "gt_manifest.json":
            continue
        session = session_name(member.path)
        document = _read_json(source, member)
        if not session or document is None:
            continue
        processes = document.get("processes")
        if not isinstance(processes, list):
            continue
        for process in processes:
            if not isinstance(process, dict):
                continue
            rows = process.get("executions")
            if not isinstance(rows, list):
                continue
            for row in rows:
                if isinstance(row, dict):
                    executions.append({
                        "session_id": session,
                        "process_code": row.get("code") or process.get("code"),
                        "case_id": row.get("case_id"),
                        "start": row.get("start_ts"),
                        "end": row.get("end_ts"),
                    })
    return executions


def _overlap_seconds(
    left_start: Any, left_end: Any, right_start: Any, right_end: Any
) -> float:
    starts = (_parse_time(left_start), _parse_time(right_start))
    ends = (_parse_time(left_end), _parse_time(right_end))
    if None in starts or None in ends:
        return 0.0
    return max(0.0, min(ends) - max(starts))  # type: ignore[arg-type]


def ground_truth_reconciliation(
    executions: Iterable[dict[str, Any]], fragments: Iterable[GroundTruthFragment]
) -> dict[str, Any]:
    """Check whether each manifest execution is covered by matching GT fragments."""
    fragment_rows = list(fragments)
    execution_rows = list(executions)
    matched = 0
    duration_ratios: list[float] = []
    unmatched_examples: list[dict[str, Any]] = []
    for execution in execution_rows:
        start = _parse_time(execution.get("start"))
        end = _parse_time(execution.get("end"))
        duration = max(0.0, end - start) if start is not None and end is not None else 0.0
        candidates = [
            fragment for fragment in fragment_rows
            if fragment.session_id == execution.get("session_id")
            and fragment.process_code == str(execution.get("process_code"))
            and (
                not execution.get("case_id")
                or not fragment.case_id
                or fragment.case_id == execution.get("case_id")
            )
        ]
        overlap = sum(
            _overlap_seconds(execution.get("start"), execution.get("end"), fragment.start, fragment.end)
            for fragment in candidates
        )
        ratio = min(1.0, overlap / duration) if duration > 0 else 0.0
        duration_ratios.append(ratio)
        if ratio >= 0.95:
            matched += 1
        elif len(unmatched_examples) < 10:
            unmatched_examples.append({**execution, "coverage_ratio": round(ratio, 4)})
    ratios_sorted = sorted(duration_ratios)
    return {
        "manifest_executions": len(execution_rows),
        "transition_fragments": len(fragment_rows),
        "executions_with_at_least_95pct_coverage": matched,
        "match_rate": round(matched / len(execution_rows), 4) if execution_rows else None,
        "median_duration_coverage": round(
            ratios_sorted[len(ratios_sorted) // 2], 4
        ) if ratios_sorted else None,
        "unmatched_examples": unmatched_examples,
    }


def validate_reconstruction(path: str | Path) -> dict[str, Any]:
    sessions = reconstruct_sessions(str(path))
    all_events = [event for session in sessions for event in session.events]
    dataset_a_fragments = [
        fragment for session in sessions if session.dataset == "dataset_a"
        for fragment in session.ground_truth
    ]
    source = DataSource(path)
    return {
        "timestamp_reversals": reversal_diagnostics(all_events),
        "ground_truth_reconciliation": ground_truth_reconciliation(
            _manifest_executions(source), dataset_a_fragments
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate reconstructed sessions")
    parser.add_argument("data_parent")
    parser.add_argument("--output")
    args = parser.parse_args()
    result = validate_reconstruction(args.data_parent)
    rendered = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)


if __name__ == "__main__":
    main()
