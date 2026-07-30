from __future__ import annotations

import pytest
from vntdr.config import Settings
from vntdr.models import BarRecord
from vntdr.services.research import ResearchService
from vntdr.storage.database import Database
from vntdr.storage.repositories import MarketDataRepository, ResearchRunRepository

def test_trade_mode_filtering(tmp_path, env_map, sample_xau_bar_payloads):
    db_path = tmp_path / "research.sqlite3"
    database = Database(f"sqlite+pysqlite:///{db_path}")
    database.create_schema()
    repository = MarketDataRepository(database)
    repository.upsert_bars_from_payloads(sample_xau_bar_payloads)
    
    bars = [BarRecord.model_validate(p) for p in sample_xau_bar_payloads]
    parameters = {
        "fast_length": 3,
        "slow_length": 6,
        "signal_length": 3,
        "trend_window": 3,
    }
    
    # Direction is a strategy parameter, not a process-wide setting.
    # 1. Both Long and Short (default)
    settings = Settings.from_mapping({
        **env_map,
        "VNTDR_DATABASE_URL": f"sqlite+pysqlite:///{db_path}",
    })
    service = ResearchService(
        settings=settings,
        market_data_repository=repository,
        research_run_repository=ResearchRunRepository(database),
    )
    
    outcome_both = service._execute_backtest(bars, "cm_macd_ult_mtf", parameters)
    assert 1 in outcome_both.signals
    assert -1 in outcome_both.signals
    
    # 2. Long Only
    outcome_long = service._execute_backtest(
        bars, "cm_macd_ult_mtf", {**parameters, "trade_mode": "long_only"}
    )
    assert 1 in outcome_long.signals
    assert -1 not in outcome_long.signals
    
    # 3. Short Only
    outcome_short = service._execute_backtest(
        bars, "cm_macd_ult_mtf", {**parameters, "trade_mode": "short_only"}
    )
    assert 1 not in outcome_short.signals
    assert -1 in outcome_short.signals
