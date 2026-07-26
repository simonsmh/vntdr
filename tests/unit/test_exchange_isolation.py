from __future__ import annotations

from datetime import datetime, timezone

from vntdr.models import BarRecord
from vntdr.storage.database import Database
from vntdr.storage.repositories import MarketDataRepository


def test_exchange_filter_prevents_proxy_and_executable_bars_from_mixing() -> None:
    database = Database("sqlite://")
    database.create_schema()
    repository = MarketDataRepository(database)
    at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    repository.upsert_bars([
        BarRecord(symbol="QQQ", exchange="TRADINGVIEW", interval="1h", datetime=at, open=1, high=1, low=1, close=1),
        BarRecord(symbol="QQQ", exchange="OKX", interval="1h", datetime=at, open=2, high=2, low=2, close=2),
    ])

    bars = repository.fetch_bars("QQQ", "1h", at, at, exchange="TRADINGVIEW")
    latest = repository.fetch_latest_bars("QQQ", "1h", limit=1, exchange="OKX")

    assert [(bar.exchange, bar.close) for bar in bars] == [("TRADINGVIEW", 1)]
    assert [(bar.exchange, bar.close) for bar in latest] == [("OKX", 2)]
