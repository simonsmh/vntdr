from __future__ import annotations

from datetime import datetime, timedelta, timezone

from vntdr.models import BarRecord, FactorObservation, Instrument
from vntdr.services.data_context import MarketDataContext
from vntdr.strategies.multi_factor import Strategy
from vntdr.strategies.multi_factor import DEFAULT_PARAMETER_SPACE


def _bars() -> list[BarRecord]:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return [BarRecord(symbol="XAU-USDT-SWAP", interval="4h", datetime=start + timedelta(hours=4 * i), open=100 + i, high=101 + i, low=99 + i, close=100.5 + i, volume=10) for i in range(80)]


def test_multi_factor_requires_closed_history_then_scores_a_trend_breakout() -> None:
    bars = _bars()
    assert Strategy.signal_for_index(bars, 10, {}) == 0
    score, explanation = Strategy.score_for_index(bars, 60, {})
    assert score > 0.6
    assert explanation["trend"] > 0
    assert explanation["momentum"] > 0
    assert explanation["regime"] == 1.0
    assert explanation["volatility"] == 1.0
    assert Strategy.signal_for_index(bars, 60, {}) == 1


def test_exit_threshold_holds_position_in_hysteresis_band() -> None:
    bars = _bars()
    parameters = {"entry_threshold": 1.1, "exit_threshold": 0.1}
    score, _ = Strategy.score_for_index(bars, 60, parameters)
    assert 0.1 < score < 1.1
    assert Strategy.target_position_for_index(bars, 60, parameters, 1) == 1
    assert Strategy.target_position_for_index(bars, 60, parameters, 0) == 0


def test_funding_factor_uses_only_available_observation_to_reduce_crowded_long() -> None:
    bars = _bars()
    decision_at = bars[60].datetime + timedelta(hours=4)
    context = MarketDataContext(
        {"4h": bars},
        factors=[FactorObservation(
            instrument=Instrument(symbol="XAU-USDT-SWAP", exchange="OKX"),
            factor_name="okx_funding_rate", value=0.001,
            observed_at=decision_at - timedelta(hours=1), available_at=decision_at - timedelta(minutes=1),
        )],
    )

    signal = Strategy.target_position_for_context(
        bars, 60, {"funding_weight": 0.5}, 0, context
    )

    assert signal == 0


def test_walk_forward_search_space_is_preregistered_and_small() -> None:
    combinations = 1
    for values in DEFAULT_PARAMETER_SPACE.values():
        combinations *= len(values)
    assert combinations == 64


def test_old_parameter_overrides_keep_new_safe_defaults(tmp_path, env_map) -> None:
    from vntdr.config import Settings
    from vntdr.services.research import ResearchService
    from vntdr.storage.database import Database
    from vntdr.storage.repositories import MarketDataRepository, ResearchRunRepository

    url = f"sqlite+pysqlite:///{tmp_path / 'defaults.sqlite'}"
    settings = Settings.from_mapping({**env_map, "VNTDR_DATABASE_URL": url})
    settings.research.strategy_parameters = {"multi_factor": {"trend_window": 80}}
    database = Database(url)
    database.create_schema()
    service = ResearchService(
        settings=settings, market_data_repository=MarketDataRepository(database),
        research_run_repository=ResearchRunRepository(database),
    )

    parameters = service.default_parameters("multi_factor")
    assert parameters["trend_window"] == 80
    assert parameters["enable_atr_sizing"] is True
