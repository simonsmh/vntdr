"""Resolve immutable strategy versions for a concrete execution instance."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from vntdr.models import StrategyInstance, StrategyVersion
from vntdr.storage.repositories import StrategyRepository


@dataclass(frozen=True)
class ResolvedStrategy:
    instance: StrategyInstance
    version: StrategyVersion


class StrategyRuntimeService:
    def __init__(self, repository: StrategyRepository) -> None:
        self.repository = repository

    def resolve(self, instance_id: UUID | str, at: datetime) -> ResolvedStrategy:
        instance = self.repository.get_instance(str(instance_id))
        if instance is None:
            raise ValueError(f"Unknown strategy instance: {instance_id}")
        if not instance.enabled:
            raise ValueError(f"Strategy instance is disabled: {instance.name}")
        version = self.repository.active_version(str(instance.id), at)
        if version is None:
            raise ValueError(f"No active strategy version for {instance.name} at {at.isoformat()}")
        return ResolvedStrategy(instance=instance, version=version)
