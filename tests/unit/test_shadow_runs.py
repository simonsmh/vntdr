from datetime import datetime, timedelta, timezone

import pytest

from vntdr.models import Instrument, Interval, ShadowRun, StrategyInstance, StrategyVersion
from vntdr.storage.database import Database
from vntdr.storage.repositories import StrategyRepository


def test_shadow_run_records_equity_and_peak_drawdown() -> None:
    database = Database("sqlite://")
    database.create_schema()
    repository = StrategyRepository(database)
    instance = repository.create_instance(StrategyInstance(
        name="gold-shadow", instrument=Instrument(symbol="TV:XAUUSD", exchange="TRADINGVIEW", asset_class="commodity"),
        primary_interval=Interval(value="4h"),
    ))
    version = repository.create_version(StrategyVersion(strategy_name="multi_factor"))
    started = datetime(2026, 1, 1, tzinfo=timezone.utc)
    run = repository.create_shadow_run(ShadowRun(
        instance_id=instance.id, strategy_version_id=version.id, started_at=started,
    ))

    recorded = repository.record_shadow_equity(str(run.id), 1.2, started + timedelta(days=1))
    with pytest.raises(ValueError, match="28 days"):
        repository.finalize_shadow_run(str(run.id), "passed")
    recorded = repository.record_shadow_equity(str(run.id), 1.08, started + timedelta(days=29))

    assert recorded.observation_count == 2
    assert recorded.peak_equity == 1.2
    assert recorded.max_drawdown == pytest.approx(-0.1)
    assert repository.finalize_shadow_run(str(run.id), "passed").status == "passed"
    with pytest.raises(ValueError, match="completed"):
        repository.record_shadow_equity(str(run.id), 1.1, started + timedelta(days=3))
