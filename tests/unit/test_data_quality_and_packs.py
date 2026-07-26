from __future__ import annotations

from datetime import datetime, timedelta, timezone

from vntdr.factors import crypto_pack, equity_index_proxy_pack, gold_pack
from vntdr.models import BarRecord
from vntdr.services.data_quality import assess_bars


def test_data_quality_rejects_gaps_and_stale_bars() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    bars = [
        BarRecord(symbol="BTC", interval="1h", datetime=start, open=1, high=1, low=1, close=1),
        BarRecord(symbol="BTC", interval="1h", datetime=start + timedelta(hours=3), open=1, high=1, low=1, close=1),
    ]
    report = assess_bars(bars, "1h", start + timedelta(hours=3))
    assert report.gaps_detected == 1
    assert report.usable is False
    assert assess_bars(bars[:1], "1h", start + timedelta(hours=3)).stale is True


def test_asset_packs_are_explicit_and_have_distinct_equity_trend_horizon() -> None:
    assert gold_pack().asset_class == "commodity"
    assert crypto_pack().asset_class == "crypto"
    assert equity_index_proxy_pack().factors[0].window == 50


def test_weekday_calendar_accepts_expected_session_closure() -> None:
    friday = datetime(2026, 1, 2, 20, tzinfo=timezone.utc)
    monday = datetime(2026, 1, 5, 14, tzinfo=timezone.utc)
    bars = [
        BarRecord(symbol="QQQ", interval="1h", datetime=friday, open=1, high=1, low=1, close=1),
        BarRecord(symbol="QQQ", interval="1h", datetime=monday, open=1, high=1, low=1, close=1),
    ]

    report = assess_bars(bars, "1h", monday, calendar="weekday")

    assert report.gaps_detected == 0
    assert report.usable
