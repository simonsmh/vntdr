"""Persist external factor observations through one auditable sync boundary."""
from __future__ import annotations

from datetime import datetime

from vntdr.models import Instrument
from vntdr.services.external_factors import ExternalFactorProvider
from vntdr.storage.repositories import StrategyRepository


class FactorSyncService:
    def __init__(self, *, repository: StrategyRepository) -> None:
        self.repository = repository

    def sync(
        self,
        *,
        provider: ExternalFactorProvider,
        instrument: Instrument,
        start: datetime,
        end: datetime,
    ) -> int:
        observations = provider.fetch(instrument=instrument, start=start, end=end)
        for observation in observations:
            self.repository.upsert_factor(observation)
        return len(observations)
