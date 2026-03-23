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
