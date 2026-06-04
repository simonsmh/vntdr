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
    
    # 1. Both Long and Short (default)
    settings = Settings.from_mapping({
        **env_map,
        "VNTDR_DATABASE_URL": f"sqlite+pysqlite:///{db_path}",
        "VNTDR_TRADE_MODE": "both",
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
    settings_long = Settings.from_mapping({
        **env_map,
        "VNTDR_DATABASE_URL": f"sqlite+pysqlite:///{db_path}",
        "VNTDR_TRADE_MODE": "long_only",
    })
    service_long = ResearchService(
        settings=settings_long,
        market_data_repository=repository,
        research_run_repository=ResearchRunRepository(database),
    )
    outcome_long = service_long._execute_backtest(bars, "cm_macd_ult_mtf", parameters)
    assert 1 in outcome_long.signals
    assert -1 not in outcome_long.signals
    
    # 3. Short Only
    settings_short = Settings.from_mapping({
        **env_map,
        "VNTDR_DATABASE_URL": f"sqlite+pysqlite:///{db_path}",
        "VNTDR_TRADE_MODE": "short_only",
    })
    service_short = ResearchService(
        settings=settings_short,
        market_data_repository=repository,
        research_run_repository=ResearchRunRepository(database),
    )
    outcome_short = service_short._execute_backtest(bars, "cm_macd_ult_mtf", parameters)
    assert 1 not in outcome_short.signals
    assert -1 in outcome_short.signals
