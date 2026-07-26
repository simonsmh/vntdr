from __future__ import annotations

from datetime import datetime, timezone

from vntdr.config import Settings
from vntdr.models import ResearchJobConfig
from vntdr.services.research import ResearchService
from vntdr.storage.database import Database
from vntdr.storage.repositories import MarketDataRepository, ResearchRunRepository


def test_ablation_uses_identical_bars_with_explicit_non_optimized_variants(tmp_path, env_map, sample_xau_bar_payloads) -> None:
    db = Database(f"sqlite+pysqlite:///{tmp_path / 'test.sqlite'}")
    db.create_schema()
    market = MarketDataRepository(db)
    market.upsert_bars_from_payloads(sample_xau_bar_payloads)
    settings = Settings.from_mapping({**env_map, "VNTDR_DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'test.sqlite'}"})
    service = ResearchService(settings=settings, market_data_repository=market, research_run_repository=ResearchRunRepository(db))
    result = service.factor_ablation(
        ResearchJobConfig(strategy_name="multi_factor", symbol="XAUUSDT", interval="4h", start=datetime(2026, 1, 1, tzinfo=timezone.utc), end=datetime(2026, 1, 4, 20, tzinfo=timezone.utc)),
        {"baseline": {}, "without_momentum": {"momentum_weight": 0.0}},
    )
    assert [item["name"] for item in result.variants] == ["baseline", "without_momentum"]
    assert result.variants[1]["parameters"]["momentum_weight"] == 0.0
