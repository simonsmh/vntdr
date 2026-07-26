from __future__ import annotations

from vntdr.webapp import STRATEGY_PARAMS, _auto_fit_parameter_space, _parse_space_value


def test_platform_instances_dataframe_shows_pending_versions(tmp_path, env_map, monkeypatch) -> None:
    from types import SimpleNamespace

    from vntdr.config import Settings
    from vntdr.models import Instrument, Interval, StrategyInstance
    from vntdr.storage.database import Database
    from vntdr.storage.repositories import StrategyRepository
    import vntdr.webapp as webapp

    url = f"sqlite+pysqlite:///{tmp_path / 'platform.sqlite'}"
    settings = Settings.from_mapping({**env_map, "VNTDR_DATABASE_URL": url})
    database = Database(url)
    database.create_schema()
    StrategyRepository(database).create_instance(StrategyInstance(
        name="gold", instrument=Instrument(symbol="XAU-USDT-SWAP", exchange="OKX", asset_class="commodity"), primary_interval=Interval(value="4h"),
    ))
    monkeypatch.setattr(webapp, "_get_config_service", lambda: SimpleNamespace(settings=settings))
    frame = webapp._platform_instances_df()
    assert frame.iloc[0]["生效策略"] == "待审批"


def test_shadow_runs_dataframe_shows_auditable_performance(tmp_path, env_map, monkeypatch) -> None:
    from types import SimpleNamespace

    from vntdr.config import Settings
    from vntdr.models import Instrument, Interval, ShadowRun, StrategyInstance, StrategyVersion
    from vntdr.storage.database import Database
    from vntdr.storage.repositories import StrategyRepository
    import vntdr.webapp as webapp

    url = f"sqlite+pysqlite:///{tmp_path / 'shadow.sqlite'}"
    settings = Settings.from_mapping({**env_map, "VNTDR_DATABASE_URL": url})
    database = Database(url)
    database.create_schema()
    repository = StrategyRepository(database)
    instance = repository.create_instance(StrategyInstance(
        name="shadow", instrument=Instrument(symbol="TV:XAUUSD", exchange="TRADINGVIEW", asset_class="commodity"),
        primary_interval=Interval(value="4h"),
    ))
    version = repository.create_version(StrategyVersion(strategy_name="multi_factor"))
    repository.create_shadow_run(ShadowRun(instance_id=instance.id, strategy_version_id=version.id))
    monkeypatch.setattr(webapp, "_get_config_service", lambda: SimpleNamespace(settings=settings))

    frame = webapp._shadow_runs_df()
    assert frame.iloc[0]["状态"] == "active"


def test_data_health_dataframe_exposes_opening_gate(tmp_path, env_map, monkeypatch) -> None:
    from datetime import datetime, timedelta, timezone
    from types import SimpleNamespace

    from vntdr.config import Settings
    from vntdr.models import BarRecord, Instrument, Interval, StrategyInstance
    from vntdr.storage.database import Database
    from vntdr.storage.repositories import MarketDataRepository, StrategyRepository
    import vntdr.webapp as webapp

    url = f"sqlite+pysqlite:///{tmp_path / 'health.sqlite'}"
    settings = Settings.from_mapping({**env_map, "VNTDR_DATABASE_URL": url})
    database = Database(url)
    database.create_schema()
    StrategyRepository(database).create_instance(StrategyInstance(
        name="health", instrument=Instrument(symbol="BTC", exchange="OKX"), primary_interval=Interval(value="1h"),
    ))
    now = datetime.now(timezone.utc)
    MarketDataRepository(database).upsert_bars([
        BarRecord(symbol="BTC", exchange="OKX", interval="1h", datetime=now - timedelta(hours=199 - index), open=1, high=1, low=1, close=1)
        for index in range(200)
    ])
    monkeypatch.setattr(webapp, "_get_config_service", lambda: SimpleNamespace(settings=settings))

    frame = webapp._data_health_df()
    assert frame.iloc[0]["数据门"] == "可用"

def test_parse_space_value_discrete() -> None:
    assert _parse_space_value("4,6,8") == [4, 6, 8]
    assert _parse_space_value(" 1, 2.5, 3 ") == [1, 2.5, 3]
    assert _parse_space_value(10) == [10]

def test_parse_space_value_ranges() -> None:
    assert _parse_space_value("4~8") == [4, 5, 6, 7, 8]
    assert _parse_space_value("4-8") == [4, 5, 6, 7, 8]
    assert _parse_space_value("4 to 8") == [4, 5, 6, 7, 8]

def test_parse_space_value_steps() -> None:
    assert _parse_space_value("4~8:2") == [4, 6, 8]
    assert _parse_space_value("4~8 step 2") == [4, 6, 8]
    assert _parse_space_value("4~8/2") == [4, 6, 8]
    assert _parse_space_value("5 to 15 step 3") == [5, 8, 11, 14]

def test_parse_space_value_floats() -> None:
    # Use approximate comparison for floats to avoid floating point accuracy issues
    parsed = _parse_space_value("1.0~1.3:0.1")
    assert len(parsed) == 4
    assert abs(parsed[0] - 1.0) < 1e-9
    assert abs(parsed[1] - 1.1) < 1e-9
    assert abs(parsed[2] - 1.2) < 1e-9
    assert abs(parsed[3] - 1.3) < 1e-9


def test_auto_fit_uses_bounded_recommended_space_not_full_bounds() -> None:
    space = _auto_fit_parameter_space("cm_macd_ult_mtf")

    assert space == {
        "fast_length": [2, 4, 6, 8, 10, 12],
        "slow_length": [10, 15, 20, 25, 30],
        "signal_length": [3, 5, 7, 9],
        "trend_window": [3, 5, 7, 9],
    }
    combinations = 1
    for values in space.values():
        combinations *= len(values)
    assert combinations == 480


def test_multi_factor_is_available_in_research_ui_with_daily_trend_space() -> None:
    assert "multi_factor" in STRATEGY_PARAMS
    assert STRATEGY_PARAMS["multi_factor"]["defaults"]["daily_trend_weight"] == 0.0
    assert _auto_fit_parameter_space("multi_factor")["daily_trend_weight"] == [0.0, 0.25, 0.5]


def test_multi_factor_ui_exposes_derivatives_and_turnover_controls() -> None:
    parameters = STRATEGY_PARAMS["multi_factor"]
    assert parameters["defaults"]["min_holding_bars"] == 3
    assert parameters["defaults"]["cooldown_bars"] == 2
    assert _auto_fit_parameter_space("multi_factor")["funding_weight"] == [0.0]
    assert "funding_weight" in parameters["bounds"]
