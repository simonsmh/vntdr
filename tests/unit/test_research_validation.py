from __future__ import annotations

from datetime import datetime, timedelta, timezone

from vntdr.config import Settings
from vntdr.models import ResearchJobConfig, ResearchReport
from vntdr.services.research import ResearchService
from vntdr.storage.database import Database
from vntdr.storage.repositories import MarketDataRepository, ResearchRunRepository


def test_validation_gate_rejects_negative_out_of_sample_return(tmp_path, env_map, monkeypatch) -> None:
    url = f"sqlite+pysqlite:///{tmp_path / 'validation.sqlite'}"
    database = Database(url)
    database.create_schema()
    service = ResearchService(
        settings=Settings.from_mapping({**env_map, "VNTDR_DATABASE_URL": url}),
        market_data_repository=MarketDataRepository(database),
        research_run_repository=ResearchRunRepository(database),
    )
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    backtest = ResearchReport(strategy_name="demo_momentum", symbol="BTC", interval="1h", mode="backtest", metrics={"trade_count": 20})
    walk = ResearchReport(strategy_name="demo_momentum", symbol="BTC", interval="1h", mode="walk-forward", metrics={"total_return": -0.01, "max_drawdown": -0.05}, fold_results=[])
    monkeypatch.setattr(service, "backtest", lambda _: backtest)
    monkeypatch.setattr(service, "walk_forward", lambda _: walk)
    bt_config = ResearchJobConfig(strategy_name="demo_momentum", symbol="BTC", interval="1h", start=now, end=now + timedelta(days=1))
    wf_config = ResearchJobConfig(strategy_name="demo_momentum", symbol="BTC", interval="1h", start=now, end=now + timedelta(days=1), mode="walk-forward", parameter_space={"lookback": [3]}, train_window=1, test_window=1)

    result = service.validate_candidate(
        backtest_config=bt_config,
        walk_forward_config=wf_config,
        minimum_fold_count=1,
    )

    assert result.passed is False
    assert "walk_forward_fold_count<1" in result.reasons
    assert "walk_forward_total_return<=0" in result.reasons
