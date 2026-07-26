from __future__ import annotations

from datetime import datetime, timezone

from vntdr.cleaning import clean_bars


def test_clean_bars_sorts_deduplicates_and_fills_gaps() -> None:
    raw_bars = [
        {
            "symbol": "BTC-USDT-SWAP",
            "exchange": "OKX",
            "interval": "1m",
            "datetime": "2026-01-01T00:02:00+00:00",
            "open": 102,
            "high": 103,
            "low": 101,
            "close": 102,
            "volume": 11,
        },
        {
            "symbol": "BTC-USDT-SWAP",
            "exchange": "OKX",
            "interval": "1m",
            "datetime": "2026-01-01T00:00:00+00:00",
            "open": 100,
            "high": 101,
            "low": 99,
            "close": 100,
            "volume": 9,
        },
        {
            "symbol": "BTC-USDT-SWAP",
            "exchange": "OKX",
            "interval": "1m",
            "datetime": "2026-01-01T00:02:00+00:00",
            "open": 103,
            "high": 104,
            "low": 102,
            "close": 103,
            "volume": 12,
        },
    ]

    result = clean_bars(raw_bars, interval="1m", fill_missing=True)

    assert result.duplicates_removed == 1
    assert result.gaps_detected == 1
    assert result.gaps_filled == 1
    assert [bar.datetime for bar in result.bars] == [
        datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc),
        datetime(2026, 1, 1, 0, 2, tzinfo=timezone.utc),
    ]
    assert result.bars[1].is_synthetic is True
    assert result.bars[1].open == 100
    assert result.bars[2].close == 103


def test_clean_bars_does_not_fill_weekend_for_weekday_calendar() -> None:
    payloads = [
        {
            "symbol": "TV:QQQ", "exchange": "TRADINGVIEW", "interval": "1h",
            "datetime": "2026-01-02T20:00:00+00:00",
            "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1,
        },
        {
            "symbol": "TV:QQQ", "exchange": "TRADINGVIEW", "interval": "1h",
            "datetime": "2026-01-05T14:00:00+00:00",
            "open": 2, "high": 2, "low": 2, "close": 2, "volume": 1,
        },
    ]

    result = clean_bars(
        payloads,
        interval="1h",
        fill_missing=True,
        calendar="weekday",
    )

    assert result.gaps_detected == 0
    assert result.gaps_filled == 0
    assert len(result.bars) == 2
