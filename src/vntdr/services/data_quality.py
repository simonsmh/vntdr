"""Deterministic data-health gate used before opening new positions."""
from __future__ import annotations

from datetime import datetime, timedelta

from vntdr.cleaning import _is_open_gap
from vntdr.models import BarRecord, DataQualityReport, Interval


def assess_bars(
    bars: list[BarRecord],
    interval: str,
    checked_at: datetime,
    *,
    minimum_bars: int = 1,
    max_stale_intervals: int = 2,
    calendar: str = "continuous",
) -> DataQualityReport:
    normalized = Interval(value=interval)
    if len(bars) < minimum_bars:
        return DataQualityReport(
            interval=normalized, checked_at=checked_at, bar_count=len(bars), usable=False,
            reason=f"requires at least {minimum_bars} bars",
        )
    ordered = sorted(bars, key=lambda bar: bar.datetime)
    gaps = sum(
        1
        for prior, current in zip(ordered, ordered[1:], strict=False)
        if (current.datetime - prior.datetime).total_seconds() > normalized.seconds * 1.5
        and _is_open_gap(
            prior.datetime,
            current.datetime,
            timedelta(seconds=normalized.seconds),
            calendar,
        )
    )
    age = (checked_at - ordered[-1].datetime).total_seconds()
    stale = age > normalized.seconds * max_stale_intervals
    reason = "stale data" if stale else "bar gaps" if gaps else None
    return DataQualityReport(
        interval=normalized, checked_at=checked_at, bar_count=len(bars), gaps_detected=gaps,
        stale=stale, usable=not stale and gaps == 0, reason=reason,
    )
