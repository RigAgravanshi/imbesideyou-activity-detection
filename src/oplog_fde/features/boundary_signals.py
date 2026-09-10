"""Measure observable event signals around Dataset A ground-truth boundaries."""

from __future__ import annotations

import argparse
from collections import Counter
import json
import re
from pathlib import Path
from typing import Any, Iterable

from oplog_fde.ingestion.sessionize import EventRecord, ReconstructedSession, reconstruct_sessions


COMPLETION_WORDS = (
    "submit", "submitted", "confirm", "confirmed", "complete", "completed",
    "save", "saved", "finish", "finished", "送信", "確定", "完了", "保存", "登録",
)
ID_PATTERN = re.compile(r"(?<![A-Za-z0-9])[A-Za-z]{1,10}[-_][A-Za-z0-9][A-Za-z0-9_-]{2,}(?![A-Za-z0-9])")


def _timestamp(record: EventRecord) -> int | None:
    value = record.data.get("timestamp_ms")
    return int(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def _app(record: EventRecord) -> str | None:
    context = record.data.get("context")
    active = context.get("active_app") if isinstance(context, dict) else None
    if not isinstance(active, dict):
        return None
    value = active.get("app_name") or active.get("process_name")
    return str(value).strip().lower() if value else None


def _page(record: EventRecord) -> str | None:
    context = record.data.get("context")
    if not isinstance(context, dict):
        return None
    tab = context.get("active_browser_tab")
    if isinstance(tab, dict):
        value = tab.get("url") or tab.get("title")
        if value:
            return str(value).strip().lower()
    active = context.get("active_app")
    if isinstance(active, dict) and active.get("window_title"):
        return str(active["window_title"]).strip().lower()
    return None


def _searchable_text(record: EventRecord) -> str:
    event = record.data
    selected: list[Any] = [event.get("event_type"), event.get("payload")]
    context = event.get("context")
    if isinstance(context, dict):
        selected.extend((context.get("extracted_text"), context.get("active_browser_tab")))
        active = context.get("active_app")
        if isinstance(active, dict):
            selected.append(active.get("window_title"))
    return json.dumps(selected, ensure_ascii=False, sort_keys=True, default=str).lower()


def _identifiers(record: EventRecord) -> set[str]:
    return {match.group(0).upper() for match in ID_PATTERN.finditer(_searchable_text(record))}


def transition_signals(previous: EventRecord, current: EventRecord) -> dict[str, bool]:
    """Return domain-independent signals for one adjacent event transition."""
    previous_ts, current_ts = _timestamp(previous), _timestamp(current)
    gap = max(0, current_ts - previous_ts) if previous_ts is not None and current_ts is not None else 0
    previous_ids, current_ids = _identifiers(previous), _identifiers(current)
    current_text = _searchable_text(current)
    previous_app, current_app = _app(previous), _app(current)
    previous_page, current_page = _page(previous), _page(current)
    return {
        "gap_ge_5s": gap >= 5_000,
        "gap_ge_30s": gap >= 30_000,
        "app_changed": bool(previous_app and current_app and previous_app != current_app),
        "page_changed": bool(previous_page and current_page and previous_page != current_page),
        "browser_navigation": current.data.get("event_type") == "browser_navigation",
        "completion_language": any(word in current_text for word in COMPLETION_WORDS),
        "identifier_changed": bool(current_ids and current_ids != previous_ids),
    }


def _iso_ms(value: str) -> int:
    from datetime import datetime

    return round(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def _true_boundary_times(session: ReconstructedSession) -> list[int]:
    starts = sorted({_iso_ms(fragment.start) for fragment in session.ground_truth})
    if not starts:
        return []
    event_times = [time for event in session.activity_events if (time := _timestamp(event)) is not None]
    if not event_times:
        return starts
    # A process start at the recording start is not an internal segmentation boundary.
    first_event = min(event_times)
    return [start for start in starts if abs(start - first_event) > 1_000]


def analyse_boundary_signals(
    sessions: Iterable[ReconstructedSession], tolerance_ms: int = 1_000
) -> dict[str, Any]:
    positive = Counter()
    negative = Counter()
    positive_count = negative_count = 0
    sessions_used = 0
    for session in sessions:
        if session.dataset != "dataset_a" or not session.ground_truth:
            continue
        events = session.activity_events
        boundaries = _true_boundary_times(session)
        if len(events) < 2 or not boundaries:
            continue
        sessions_used += 1
        transition_times = [_timestamp(event) for event in events[1:]]
        positive_indexes: set[int] = set()
        for boundary in boundaries:
            candidates = [
                (abs(timestamp - boundary), index)
                for index, timestamp in enumerate(transition_times)
                if timestamp is not None and abs(timestamp - boundary) <= tolerance_ms
            ]
            if candidates:
                positive_indexes.add(min(candidates)[1])
        for index, (previous, current) in enumerate(zip(events, events[1:])):
            current_ts = _timestamp(current)
            if current_ts is None:
                continue
            is_boundary = index in positive_indexes
            signals = transition_signals(previous, current)
            target = positive if is_boundary else negative
            if is_boundary:
                positive_count += 1
            else:
                negative_count += 1
            target.update(name for name, present in signals.items() if present)

    rows = []
    for name in sorted(set(positive) | set(negative)):
        positive_rate = positive[name] / positive_count if positive_count else 0.0
        negative_rate = negative[name] / negative_count if negative_count else 0.0
        rows.append({
            "signal": name,
            "boundary_count": positive[name],
            "boundary_rate": round(positive_rate, 4),
            "non_boundary_count": negative[name],
            "non_boundary_rate": round(negative_rate, 4),
            "lift": round(positive_rate / negative_rate, 3) if negative_rate else None,
        })
    rows.sort(key=lambda row: (row["lift"] is not None, row["lift"] or 0), reverse=True)
    return {
        "tolerance_ms": tolerance_ms,
        "dataset_a_sessions_used": sessions_used,
        "boundary_transitions": positive_count,
        "non_boundary_transitions": negative_count,
        "signals": rows,
        "warning": "Exploratory evidence only; Dataset A GT is not used as a Dataset B feature.",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Profile signals near Dataset A boundaries")
    parser.add_argument("data_parent")
    parser.add_argument("--tolerance-ms", type=int, default=1_000)
    parser.add_argument("--output")
    args = parser.parse_args()
    result = analyse_boundary_signals(
        reconstruct_sessions(args.data_parent), tolerance_ms=args.tolerance_ms
    )
    rendered = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)


if __name__ == "__main__":
    main()