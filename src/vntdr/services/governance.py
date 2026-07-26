"""Human approval gate for activating immutable strategy versions."""
from __future__ import annotations

from datetime import datetime
from uuid import UUID

from vntdr.models import StrategyActivation, ValidationGate
from vntdr.storage.repositories import StrategyRepository


class StrategyGovernanceService:
    def __init__(self, repository: StrategyRepository, *, max_drawdown_limit: float = 0.10) -> None:
        self.repository = repository
        self.max_drawdown_limit = max_drawdown_limit

    def approve_activation(
        self,
        *,
        instance_id: UUID,
        version_id: UUID,
        effective_at: datetime,
        approved_by: str,
        validation: ValidationGate,
    ) -> StrategyActivation:
        if not validation.approved:
            raise ValueError("Activation requires passed backtest, walk-forward, and shadow validation")
        if validation.max_drawdown is not None and abs(validation.max_drawdown) > self.max_drawdown_limit:
            raise ValueError("Activation rejected: validation drawdown exceeds limit")
        activation = StrategyActivation(
            instance_id=instance_id,
            strategy_version_id=version_id,
            effective_at=effective_at,
            approved_by=approved_by,
        )
        return self.repository.activate(activation)

    def rollback(
        self,
        *,
        instance_id: UUID,
        target_version_id: UUID,
        effective_at: datetime,
        approved_by: str,
    ) -> StrategyActivation:
        """Re-activate a previously validated version with an audit link."""
        current = self.repository.active_version(str(instance_id), effective_at)
        if current is None:
            raise ValueError("Cannot roll back an instance with no active version")
        if current.id == target_version_id:
            raise ValueError("Rollback target is already active")
        return self.repository.activate(StrategyActivation(
            instance_id=instance_id,
            strategy_version_id=target_version_id,
            effective_at=effective_at,
            approved_by=approved_by,
            rollback_of=current.id,
        ))
