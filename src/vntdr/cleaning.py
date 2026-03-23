from __future__ import annotations

from datetime import timedelta
from typing import Any

from dateutil import parser

from vntdr.models import BarRecord, CleanBarsResult

INTERVAL_TO_DELTA = {
    "1m": timedelta(minutes=1),
    "3m": timedelta(minutes=3),
    "5m": timedelta(minutes=5),
    "15m": timedelta(minutes=15),
    "30m": timedelta(minutes=30),
    "1h": timedelta(hours=1),
    "4h": timedelta(hours=4),
    "1d": timedelta(days=1),
}


def clean_bars(
    raw_bars: list[dict[str, Any]],
    interval: str,
    fill_missing: bool = False,
) -> CleanBarsResult:
    normalized: dict[tuple[str, str, str, object], BarRecord] = {}
    duplicates_removed = 0
    for payload in raw_bars:
        normalized_payload = dict(payload)
        normalized_payload["datetime"] = parser.isoparse(str(payload["datetime"]))
        record = BarRecord.model_validate(normalized_payload)
        if record.key in normalized:
            duplicates_removed += 1
        normalized[record.key] = record

    bars = sorted(normalized.values(), key=lambda item: item.datetime)
    if not bars:
        return CleanBarsResult(bars=[], duplicates_removed=duplicates_removed)

    delta = INTERVAL_TO_DELTA.get(interval)
    if delta is None:
        raise ValueError(f"Unsupported interval: {interval}")

    gaps_detected = 0
    gaps_filled = 0
    final_bars: list[BarRecord] = [bars[0]]
    for current in bars[1:]:
        previous = final_bars[-1]
        gap_cursor = previous.datetime + delta
        if current.datetime > gap_cursor:
            gaps_detected += 1
        while fill_missing and current.datetime > gap_cursor:
            synthetic = BarRecord(
                symbol=previous.symbol,
                exchange=previous.exchange,
                interval=previous.interval,
                datetime=gap_cursor,
                open=previous.close,
                high=previous.close,
                low=previous.close,
                close=previous.close,
                volume=0.0,
                is_synthetic=True,
            )
            final_bars.append(synthetic)
            previous = synthetic
            gap_cursor = previous.datetime + delta
            gaps_filled += 1
        final_bars.append(current)

    return CleanBarsResult(
        bars=final_bars,
        duplicates_removed=duplicates_removed,
        gaps_detected=gaps_detected,
        gaps_filled=gaps_filled,
    )
