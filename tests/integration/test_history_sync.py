from __future__ import annotations

from datetime import datetime, timezone

from vntdr.config import Settings
from vntdr.services.history import HistorySyncService
from vntdr.storage.database import Database
from vntdr.storage.repositories import MarketDataRepository, ResearchRunRepository


class FlakyHistoryClient:
    def __init__(self, payloads: list[dict[str, object]]) -> None:
        self.payloads = payloads
        self.calls = 0

    def fetch_candles(
        self,
        symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
        limit: int,
    ) -> list[dict[str, object]]:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("temporary network issue")
        return self.payloads


def test_sync_history_is_idempotent_and_retries(
    tmp_path,
    env_map: dict[str, str],
    sample_bar_payloads: list[dict[str, object]],
) -> None:
    db_path = tmp_path / "research.sqlite3"
    database = Database(f"sqlite+pysqlite:///{db_path}")
    database.create_schema()
    settings = Settings.from_mapping(
        {
            **env_map,
            "VNTDR_DATABASE_URL": f"sqlite+pysqlite:///{db_path}",
        }
    )
    history_client = FlakyHistoryClient(sample_bar_payloads)
    service = HistorySyncService(
        settings=settings,
        history_client=history_client,
        market_data_repository=MarketDataRepository(database),
        research_run_repository=ResearchRunRepository(database),
    )

    first = service.sync(
        symbol="BTC-USDT-SWAP",
        interval="1m",
        start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end=datetime(2026, 1, 1, 0, 9, tzinfo=timezone.utc),
        fill_missing=False,
    )
    second = service.sync(
        symbol="BTC-USDT-SWAP",
        interval="1m",
        start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end=datetime(2026, 1, 1, 0, 9, tzinfo=timezone.utc),
        fill_missing=False,
    )

    stored = MarketDataRepository(database).fetch_bars(
        symbol="BTC-USDT-SWAP",
        interval="1m",
        start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end=datetime(2026, 1, 1, 0, 9, tzinfo=timezone.utc),
    )

    assert history_client.calls >= 3
    assert first.inserted_count == 10
    assert second.inserted_count == 0
    assert len(stored) == 10
