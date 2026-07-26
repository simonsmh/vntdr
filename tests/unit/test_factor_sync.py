from __future__ import annotations

from datetime import datetime, timedelta, timezone

from vntdr.models import FactorObservation, Instrument
from vntdr.services.factor_sync import FactorSyncService
from vntdr.storage.database import Database
from vntdr.storage.repositories import StrategyRepository


class Provider:
    def fetch(self, *, instrument, start, end):
        return [FactorObservation(
            instrument=instrument, factor_name="macro", value=1.5, observed_at=start,
            available_at=start + timedelta(days=1), metadata={"source": "test"},
        )]


def test_factor_sync_persists_provider_data_for_point_in_time_reads() -> None:
    db = Database("sqlite://")
    db.create_schema()
    repository = StrategyRepository(db)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    instrument = Instrument(symbol="XAU-USDT-SWAP", exchange="OKX", asset_class="commodity")
    assert FactorSyncService(repository=repository).sync(provider=Provider(), instrument=instrument, start=start, end=start) == 1
    assert repository.factors_available_at(instrument, start) == []
    assert repository.factors_available_at(instrument, start + timedelta(days=1))[0].value == 1.5
