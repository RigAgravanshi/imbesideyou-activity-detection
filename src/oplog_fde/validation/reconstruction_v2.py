"""Corrected reconstruction diagnostics (version 2)."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
import math
from pathlib import Path
from typing import Any

from oplog_fde.ingestion.sessionize import reconstruct_sessions
from oplog_fde.ingestion.source import DataSource
from oplog_fde.validation.reconstruction import _manifest_executions


def _time(value: Any) -> float | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _percentile(values: list[int], fraction: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def reversal_report(sessions: list[Any]) -> dict[str, Any]:
    """Reset the comparison at each session boundary."""
    magnitudes: list[int] = []
    for session in sessions:
        previous: int | float | None = None
        for record in session.events:
            current = record.data.get("timestamp_ms")
            if not isinstance(current, (int, float)) or isinstance(current, bool):
                continue
            if previous is not None and current < previous:
                magnitudes.append(int(previous - current))
            previous = current

    buckets = Counter()
    for value in magnitudes:
        if value <= 10:
            buckets["le_10_ms"] += 1
        elif value <= 100:
            buckets["11_100_ms"] += 1
        elif value <= 1000:
            buckets["101_1000_ms"] += 1
        else:
            buckets["gt_1000_ms"] += 1

    names = ("le_10_ms", "11_100_ms", "101_1000_ms", "gt_1000_ms")
    return {
        "count": len(magnitudes),
        "magnitude_ms": {
            "p50": _percentile(magnitudes, 0.50),
            "p95": _percentile(magnitudes, 0.95),
            "p99": _percentile(magnitudes, 0.99),
            "max": max(magnitudes, default=None),
        },
        "buckets": {name: buckets[name] for name in names},
    }


def gt_report(executions: list[dict[str, Any]], fragments: list[Any]) -> dict[str, Any]:
    """Score only manifest executions having valid start and end timestamps."""
    complete = matched = incomplete = 0
    coverage_values: list[float] = []
    problem_examples: list[dict[str, Any]] = []

    for execution in executions:
        start = _time(execution.get("start"))
        end = _time(execution.get("end"))
        if start is None or end is None or end <= start:
            incomplete += 1
            if len(problem_examples) < 10:
                problem_examples.append({**execution, "reason": "missing_or_invalid_interval"})
            continue

        complete += 1
        overlap = 0.0
        for fragment in fragments:
            same_identity = (
                fragment.session_id == execution.get("session_id")
                and fragment.process_code == str(execution.get("process_code"))
                and (
                    not execution.get("case_id")
                    or not fragment.case_id
                    or fragment.case_id == execution.get("case_id")
                )
            )
            if not same_identity:
                continue
            fragment_start = _time(fragment.start)
            fragment_end = _time(fragment.end)
            if fragment_start is None or fragment_end is None:
                continue
            overlap += max(0.0, min(end, fragment_end) - max(start, fragment_start))

        coverage = min(1.0, overlap / (end - start))
        coverage_values.append(coverage)
        if coverage >= 0.95:
            matched += 1
        elif len(problem_examples) < 10:
            problem_examples.append({**execution, "coverage_ratio": round(coverage, 4)})

    ordered_coverage = sorted(coverage_values)
    return {
        "manifest_executions": len(executions),
        "complete_manifest_executions_evaluated": complete,
        "incomplete_manifest_executions": incomplete,
        "transition_fragments": len(fragments),
        "executions_with_at_least_95pct_coverage": matched,
        "match_rate_among_complete_executions": round(matched / complete, 4) if complete else None,
        "median_duration_coverage": round(
            ordered_coverage[len(ordered_coverage) // 2], 4
        ) if ordered_coverage else None,
        "problem_examples": problem_examples,
    }


def validate_reconstruction_v2(path: str | Path) -> dict[str, Any]:
    sessions = reconstruct_sessions(str(path))
    fragments = [
        fragment
        for session in sessions
        if session.dataset == "dataset_a"
        for fragment in session.ground_truth
    ]
    source = DataSource(path)
    return {
        "timestamp_reversals_within_sessions": reversal_report(sessions),
        "ground_truth_reconciliation": gt_report(
            _manifest_executions(source), fragments
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate reconstructed sessions, v2")
    parser.add_argument("data_parent")
    parser.add_argument("--output")
    args = parser.parse_args()
    result = validate_reconstruction_v2(args.data_parent)
    rendered = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)


if __name__ == "__main__":
    main()
