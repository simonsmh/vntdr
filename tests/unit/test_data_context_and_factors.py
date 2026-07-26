from __future__ import annotations

from datetime import datetime, timedelta, timezone

from vntdr.factors import AtrRatioFactor, BreakoutFactor, TrendFactor
from vntdr.models import BarRecord, FactorObservation, Instrument
from vntdr.services.data_context import MarketDataContext


def test_auxiliary_bars_are_only_visible_after_their_close() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    daily = [BarRecord(symbol="QQQ", interval="1d", datetime=start, open=1, high=2, low=1, close=2)]
    context = MarketDataContext({"1d": daily})
    assert context.latest_closed_bar("1d", start + timedelta(hours=12)) is None
    assert context.latest_closed_bar("1D", start + timedelta(days=1)) == daily[0]


def test_ohlcv_factor_plugins_return_none_until_warm_and_values_afterward() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    bars = [BarRecord(symbol="BTC", interval="1h", datetime=start + timedelta(hours=i), open=100 + i, high=101 + i, low=99 + i, close=100 + i) for i in range(25)]
    assert TrendFactor(window=20).compute(bars, 19) is None
    assert TrendFactor(window=20).compute(bars, 20) > 0
    assert BreakoutFactor(window=20).compute(bars, 20) == 1.0
    assert AtrRatioFactor(window=14).compute(bars, 20) is not None


def test_closed_daily_context_excludes_the_in_progress_daily_bar() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    daily = [
        BarRecord(symbol="XAU", interval="1d", datetime=start, open=1, high=1, low=1, close=1),
        BarRecord(symbol="XAU", interval="1d", datetime=start + timedelta(days=1), open=9, high=9, low=9, close=9),
    ]
    context = MarketDataContext({"1d": daily})

    visible = context.closed_bars("1d", start + timedelta(days=1, hours=12))

    assert [bar.close for bar in visible] == [1]


def test_factor_context_honours_available_at_not_only_observed_at() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    factor = FactorObservation(
        instrument=Instrument(symbol="BTC", exchange="OKX"), factor_name="okx_funding_rate",
        value=0.001, observed_at=start, available_at=start + timedelta(hours=8),
    )
    context = MarketDataContext({}, factors=[factor])

    assert context.latest_factor("okx_funding_rate", start + timedelta(hours=4)) is None
    assert context.latest_factor("okx_funding_rate", start + timedelta(hours=8)) == factor
