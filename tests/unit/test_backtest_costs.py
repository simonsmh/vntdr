from __future__ import annotations

from datetime import datetime, timedelta, timezone

from vntdr.config import Settings
from vntdr.models import BarRecord
from vntdr.services.research import ResearchService
from vntdr.storage.database import Database
from vntdr.storage.repositories import MarketDataRepository, ResearchRunRepository


class AlwaysLong:
    @classmethod
    def signal_for_index(cls, bars, index, parameters):
        return 1


def _run(tmp_path, env_map, monkeypatch, **costs):
    tmp_path.mkdir()
    url = f"sqlite+pysqlite:///{tmp_path / 'cost.sqlite'}"
    db = Database(url)
    db.create_schema()
    settings = Settings.from_mapping({**env_map, "VNTDR_DATABASE_URL": url, **costs})
    service = ResearchService(settings=settings, market_data_repository=MarketDataRepository(db), research_run_repository=ResearchRunRepository(db))
    monkeypatch.setattr(service, "_load_strategy", lambda _: AlwaysLong)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    bars = [BarRecord(symbol="BTC", interval="1h", datetime=start + timedelta(hours=i), open=100, high=101, low=99, close=100) for i in range(3)]
    return service._execute_backtest(bars, "always", {})


def test_spread_slippage_and_funding_reduce_cost_inclusive_trade_return(tmp_path, env_map, monkeypatch) -> None:
    baseline = _run(tmp_path / "base", env_map, monkeypatch, VNTDR_TAKER_FEE_RATE="0")
    costly = _run(tmp_path / "costly", env_map, monkeypatch, VNTDR_TAKER_FEE_RATE="0", VNTDR_SLIPPAGE_BPS="10", VNTDR_SPREAD_BPS="10", VNTDR_FUNDING_RATE_PER_BAR="0.001")
    assert baseline.trades[0].net_return == 0
    assert costly.trades[0].net_return < baseline.trades[0].net_return
    assert costly.trades[0].transaction_cost > 0
    assert costly.trades[0].funding_cost > 0
