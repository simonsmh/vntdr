from __future__ import annotations

from datetime import datetime, timedelta, timezone

from vntdr.config import Settings
from vntdr.models import BarRecord, ResearchJobConfig
from vntdr.services.research import ResearchService
from vntdr.storage.database import Database
from vntdr.storage.repositories import MarketDataRepository, ResearchRunRepository


class ContextAwareStrategy:
    seen_daily_closes: list[float | None] = []

    @classmethod
    def target_position_for_context(cls, bars, index, parameters, current_position, context):
        decision_at = bars[index].datetime + timedelta(hours=4)
        latest = context.latest_closed_bar("1d", decision_at)
        cls.seen_daily_closes.append(latest.close if latest else None)
        return 0


def test_research_uses_only_closed_auxiliary_bars(tmp_path, env_map, monkeypatch) -> None:
    db_url = f"sqlite+pysqlite:///{tmp_path / 'mtf.sqlite'}"
    db = Database(db_url)
    db.create_schema()
    market = MarketDataRepository(db)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    primary = [
        BarRecord(symbol="TV:XAUUSD", exchange="TRADINGVIEW", interval="4h", datetime=start + timedelta(hours=4 * i), open=100, high=101, low=99, close=100)
        for i in range(8)
    ]
    daily = [
        BarRecord(symbol="TV:XAUUSD", exchange="TRADINGVIEW", interval="1d", datetime=start, open=1, high=1, low=1, close=1),
        BarRecord(symbol="TV:XAUUSD", exchange="TRADINGVIEW", interval="1d", datetime=start + timedelta(days=1), open=9, high=9, low=9, close=9),
    ]
    market.upsert_bars(primary + daily)
    settings = Settings.from_mapping({**env_map, "VNTDR_DATABASE_URL": db_url})
    service = ResearchService(settings=settings, market_data_repository=market, research_run_repository=ResearchRunRepository(db))
    monkeypatch.setattr(service, "_load_strategy", lambda _: ContextAwareStrategy)
    ContextAwareStrategy.seen_daily_closes = []

    service.backtest_with_details(ResearchJobConfig(
        strategy_name="context_aware", symbol="TV:XAUUSD", interval="4h",
        auxiliary_intervals=["1d"], start=start, end=start + timedelta(hours=28),
    ))

    # At the first five 4h closes the Jan-2 daily bar is not finished; it
    # cannot leak its close of 9 into the strategy.
    assert ContextAwareStrategy.seen_daily_closes[:5] == [None, None, None, None, None]
    assert ContextAwareStrategy.seen_daily_closes[5:] == [1, 1]
