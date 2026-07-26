from __future__ import annotations

from datetime import datetime, timezone

import pytest

from vntdr.models import Instrument, Interval, StrategyInstance, StrategyVersion, ValidationGate
from vntdr.services.governance import StrategyGovernanceService
from vntdr.storage.database import Database
from vntdr.storage.repositories import StrategyRepository


def test_governance_requires_all_validation_gates_before_activation() -> None:
    db = Database("sqlite://")
    db.create_schema()
    repository = StrategyRepository(db)
    instance = repository.create_instance(StrategyInstance(name="btc", instrument=Instrument(symbol="BTC", exchange="OKX"), primary_interval=Interval(value="1h")))
    version = repository.create_version(StrategyVersion(strategy_name="demo_momentum"))
    governance = StrategyGovernanceService(repository)
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="requires"):
        governance.approve_activation(instance_id=instance.id, version_id=version.id, effective_at=now, approved_by="alice", validation=ValidationGate(backtest_passed=True))
    approved = governance.approve_activation(
        instance_id=instance.id, version_id=version.id, effective_at=now, approved_by="alice",
        validation=ValidationGate(backtest_passed=True, walk_forward_passed=True, shadow_passed=True, max_drawdown=-0.05),
    )
    assert approved.approved_by == "alice"


def test_governance_rolls_back_to_a_previous_version_with_audit_link() -> None:
    db = Database("sqlite://")
    db.create_schema()
    repository = StrategyRepository(db)
    instance = repository.create_instance(StrategyInstance(name="btc", instrument=Instrument(symbol="BTC", exchange="OKX"), primary_interval=Interval(value="1h")))
    first = repository.create_version(StrategyVersion(strategy_name="demo_momentum", parameters={"lookback": 3}))
    second = repository.create_version(first.clone(parameters={"lookback": 5}))
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    governance = StrategyGovernanceService(repository)
    validated = ValidationGate(backtest_passed=True, walk_forward_passed=True, shadow_passed=True)
    governance.approve_activation(instance_id=instance.id, version_id=first.id, effective_at=now, approved_by="alice", validation=validated)
    governance.approve_activation(instance_id=instance.id, version_id=second.id, effective_at=now.replace(hour=1), approved_by="alice", validation=validated)
    rollback = governance.rollback(instance_id=instance.id, target_version_id=first.id, effective_at=now.replace(hour=2), approved_by="alice")
    assert rollback.rollback_of == second.id
    assert repository.active_version(str(instance.id), now.replace(hour=3)).id == first.id
