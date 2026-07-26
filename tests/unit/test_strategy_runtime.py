from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from vntdr.models import Instrument, Interval, StrategyActivation, StrategyInstance, StrategyVersion
from vntdr.services.strategy_runtime import StrategyRuntimeService
from vntdr.storage.database import Database
from vntdr.storage.repositories import StrategyRepository


def test_runtime_resolves_only_the_version_active_at_a_closed_bar() -> None:
    db = Database("sqlite://")
    db.create_schema()
    repository = StrategyRepository(db)
    instance = repository.create_instance(StrategyInstance(
        name="btc", instrument=Instrument(symbol="BTC-USDT-SWAP", exchange="OKX"), primary_interval=Interval(value="1h"),
    ))
    first = repository.create_version(StrategyVersion(strategy_name="demo_momentum", parameters={"lookback": 3}))
    second = repository.create_version(first.clone(parameters={"lookback": 5}))
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    repository.activate(StrategyActivation(instance_id=instance.id, strategy_version_id=first.id, effective_at=start))
    repository.activate(StrategyActivation(instance_id=instance.id, strategy_version_id=second.id, effective_at=start + timedelta(hours=2)))
    runtime = StrategyRuntimeService(repository)

    assert runtime.resolve(instance.id, start + timedelta(hours=1)).version.parameters == {"lookback": 3}
    assert runtime.resolve(instance.id, start + timedelta(hours=2)).version.parameters == {"lookback": 5}
    with pytest.raises(ValueError, match="No active"):
        runtime.resolve(instance.id, start - timedelta(seconds=1))
