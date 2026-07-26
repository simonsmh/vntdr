from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from vntdr.models import FactorObservation, Instrument, Interval, StrategyActivation, StrategyInstance, StrategyVersion
from vntdr.storage.database import Database
from vntdr.storage.repositories import StrategyRepository


def test_interval_is_canonical_and_validated() -> None:
    assert Interval(value="4H").value == "4h"
    assert Interval(value="4H").seconds == 14_400
    with pytest.raises(ValueError):
        Interval(value="close")


def test_strategy_versions_are_snapshots_that_can_be_activated_and_rolled_back() -> None:
    db = Database("sqlite://")
    db.create_schema()
    repository = StrategyRepository(db)
    instance = repository.create_instance(StrategyInstance(
        name="gold-4h", instrument=Instrument(symbol="xau-usdt-swap", exchange="okx", asset_class="commodity"),
        primary_interval=Interval(value="4h"),
    ))
    original = repository.create_version(StrategyVersion(strategy_name="cm_macd_ult_mtf", parameters={"fast_length": 6}))
    revised = repository.create_version(original.clone(parameters={"fast_length": 8}))
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    repository.activate(StrategyActivation(instance_id=instance.id, strategy_version_id=original.id, effective_at=now))
    repository.activate(StrategyActivation(instance_id=instance.id, strategy_version_id=revised.id, effective_at=now + timedelta(hours=4)))

    assert repository.active_version(str(instance.id), now + timedelta(hours=3)).id == original.id
    assert repository.active_version(str(instance.id), now + timedelta(hours=5)).id == revised.id
    assert original.parameters == {"fast_length": 6}
    assert revised.parent_id == original.id


def test_factor_reads_respect_available_at_to_prevent_lookahead() -> None:
    db = Database("sqlite://")
    db.create_schema()
    repository = StrategyRepository(db)
    instrument = Instrument(symbol="XAU-USDT-SWAP", exchange="OKX", asset_class="commodity")
    observed = datetime(2026, 1, 1, tzinfo=timezone.utc)
    repository.upsert_factor(FactorObservation(
        instrument=instrument, factor_name="macro", value=1.0, observed_at=observed,
        available_at=observed + timedelta(days=1), interval=Interval(value="1d"),
    ))

    assert repository.factors_available_at(instrument, observed + timedelta(hours=12)) == []
    assert len(repository.factors_available_at(instrument, observed + timedelta(days=1))) == 1
