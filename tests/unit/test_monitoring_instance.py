from __future__ import annotations

from datetime import datetime, timedelta, timezone

from vntdr.models import BarRecord, Instrument, Interval, MonitorResult, StrategyActivation, StrategyInstance, StrategyVersion
from vntdr.services.monitoring import MonitoringService
from vntdr.services.strategy_runtime import StrategyRuntimeService
from vntdr.storage.database import Database
from vntdr.storage.repositories import StrategyRepository


class Bars:
    def __init__(self, bars):
        self.bars = bars

    def fetch_latest_bars(self, symbol, interval, limit, **kwargs):
        return self.bars


class MultiBars:
    def __init__(self, by_interval):
        self.by_interval = by_interval

    def fetch_latest_bars(self, symbol, interval, limit, **kwargs):
        return self.by_interval[interval]


def test_instance_monitoring_uses_the_version_active_when_last_bar_closed(monkeypatch) -> None:
    db = Database("sqlite://")
    db.create_schema()
    repository = StrategyRepository(db)
    instance = repository.create_instance(StrategyInstance(
        name="btc", instrument=Instrument(symbol="BTC-USDT-SWAP", exchange="OKX"), primary_interval=Interval(value="1h"),
    ))
    version = repository.create_version(StrategyVersion(strategy_name="demo_momentum", parameters={"lookback": 5}))
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    repository.activate(StrategyActivation(instance_id=instance.id, strategy_version_id=version.id, effective_at=start))
    bars = [BarRecord(symbol="BTC-USDT-SWAP", interval="1h", datetime=start + timedelta(hours=i), open=1, high=1, low=1, close=1) for i in range(3)]
    service = MonitoringService.__new__(MonitoringService)
    service.market_data_repository = Bars(bars)
    captured = {}

    def fake_monitor_once(**kwargs):
        captured.update(kwargs)
        return MonitorResult(symbol=kwargs["symbol"], interval=kwargs["interval"], strategy_name=kwargs["strategy_name"], signal=0)

    monkeypatch.setattr(service, "monitor_once", fake_monitor_once)
    result = service.monitor_instance_once(instance_id=str(instance.id), runtime=StrategyRuntimeService(repository), volume=1)

    assert captured["parameters"] == {"lookback": 5}
    assert captured["execution_mode"] == "notify_only"
    assert result.strategy_version_id == version.id


def test_instance_monitoring_passes_registered_auxiliary_bars(monkeypatch) -> None:
    db = Database("sqlite://")
    db.create_schema()
    repository = StrategyRepository(db)
    instance = repository.create_instance(StrategyInstance(
        name="xau", instrument=Instrument(symbol="TV:XAUUSD", exchange="TRADINGVIEW"),
        primary_interval=Interval(value="4h"), auxiliary_intervals=[Interval(value="1d")],
    ))
    version = repository.create_version(StrategyVersion(strategy_name="multi_factor"))
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    repository.activate(StrategyActivation(instance_id=instance.id, strategy_version_id=version.id, effective_at=start))
    primary = [BarRecord(symbol="TV:XAUUSD", interval="4h", datetime=start + timedelta(hours=4 * i), open=1, high=1, low=1, close=1) for i in range(3)]
    daily = [BarRecord(symbol="TV:XAUUSD", interval="1d", datetime=start + timedelta(days=i), open=1, high=1, low=1, close=1) for i in range(3)]
    service = MonitoringService.__new__(MonitoringService)
    service.market_data_repository = MultiBars({"4h": primary, "1d": daily})
    captured = {}

    def fake_monitor_once(**kwargs):
        captured.update(kwargs)
        return MonitorResult(symbol=kwargs["symbol"], interval=kwargs["interval"], strategy_name=kwargs["strategy_name"], signal=0)

    monkeypatch.setattr(service, "monitor_once", fake_monitor_once)
    service.monitor_instance_once(instance_id=str(instance.id), runtime=StrategyRuntimeService(repository), volume=1)

    assert captured["auxiliary_bars_by_interval"] == {"1d": daily}
    assert captured["factors"] == []
