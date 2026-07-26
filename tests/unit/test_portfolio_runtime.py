from __future__ import annotations

from types import SimpleNamespace

from vntdr.models import Instrument, Interval, MonitorResult, StrategyInstance
from vntdr.services.portfolio_runtime import PortfolioRuntimeService
from vntdr.services.strategy_runtime import StrategyRuntimeService
from vntdr.storage.database import Database
from vntdr.storage.repositories import StrategyRepository


class Monitor:
    def monitor_instance_once(self, *, instance_id, runtime, volume, lookback_bars):
        instance = runtime.repository.get_instance(instance_id)
        signal = 1 if instance.name == "btc" else -1
        return MonitorResult(symbol=instance.instrument.symbol, interval="1h", strategy_name="test", signal=signal)


def test_portfolio_runtime_aggregates_enabled_instances_and_isolates_failures() -> None:
    db = Database("sqlite://")
    db.create_schema()
    repository = StrategyRepository(db)
    for name, symbol in [("btc", "BTC-USDT-SWAP"), ("eth", "ETH-USDT-SWAP")]:
        repository.create_instance(StrategyInstance(name=name, instrument=Instrument(symbol=symbol, exchange="OKX"), primary_interval=Interval(value="1h")))
    result = PortfolioRuntimeService(
        strategy_repository=repository, strategy_runtime=StrategyRuntimeService(repository), monitoring_service=Monitor()
    ).run_enabled(volume=1)

    assert len(result.decisions) == 2
    assert result.portfolio.gross_exposure > 0
    assert result.errors == {}
