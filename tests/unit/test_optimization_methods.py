from __future__ import annotations

from datetime import datetime, timezone
import pytest

from vntdr.config import Settings
from vntdr.models import ResearchJobConfig
from vntdr.services.research import ResearchService
from vntdr.storage.database import Database
from vntdr.storage.repositories import MarketDataRepository, ResearchRunRepository
from vntdr.webapp import _auto_fit_parameter_space


def test_heuristic_and_grid_optimization_methods(
    tmp_path,
    env_map: dict[str, str],
    sample_xau_bar_payloads: list[dict[str, object]],
) -> None:
    db_path = tmp_path / "research.sqlite3"
    report_dir = tmp_path / "reports"
    database = Database(f"sqlite+pysqlite:///{db_path}")
    database.create_schema()
    repository = MarketDataRepository(database)
    repository.upsert_bars_from_payloads(sample_xau_bar_payloads)

    settings = Settings.from_mapping(
        {
            **env_map,
            "VNTDR_DATABASE_URL": f"sqlite+pysqlite:///{db_path}",
            "VNTDR_REPORT_DIR": str(report_dir),
        }
    )
    service = ResearchService(
        settings=settings,
        market_data_repository=repository,
        research_run_repository=ResearchRunRepository(database),
    )

    config = ResearchJobConfig(
        strategy_name="cm_macd_ult_mtf",
        symbol="XAUUSDT",
        interval="4h",
        start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end=datetime(2026, 1, 4, 20, 0, tzinfo=timezone.utc),
        mode="optimize",
        parameter_space={
            "fast_length": [3, 4],
            "slow_length": [6, 7],
            "signal_length": [3],
            "trend_window": [2, 3],
        },
    )

    # 1. Test Grid Search
    report_grid = service.optimize(config.model_copy(update={"method": "grid"}), method="grid")
    assert set(report_grid.best_parameters) == {"fast_length", "slow_length", "signal_length", "trend_window"}
    assert report_grid.metrics["trade_count"] >= 1
    assert len(report_grid.top_results) > 0

    # 2. Test Heuristic Search (bfs / astar)
    report_heuristic = service.optimize(config.model_copy(update={"method": "heuristic"}), method="heuristic")
    assert set(report_heuristic.best_parameters) == {"fast_length", "slow_length", "signal_length", "trend_window"}
    assert report_heuristic.metrics["trade_count"] >= 1
    assert len(report_heuristic.top_results) > 0


def test_auto_fit_space_keeps_heuristic_equal_to_grid_for_macd(
    tmp_path,
    env_map: dict[str, str],
    sample_xau_bar_payloads: list[dict[str, object]],
) -> None:
    db_path = tmp_path / "research.sqlite3"
    report_dir = tmp_path / "reports"
    database = Database(f"sqlite+pysqlite:///{db_path}")
    database.create_schema()
    repository = MarketDataRepository(database)
    repository.upsert_bars_from_payloads(sample_xau_bar_payloads)

    settings = Settings.from_mapping(
        {
            **env_map,
            "VNTDR_DATABASE_URL": f"sqlite+pysqlite:///{db_path}",
            "VNTDR_REPORT_DIR": str(report_dir),
        }
    )
    service = ResearchService(
        settings=settings,
        market_data_repository=repository,
        research_run_repository=ResearchRunRepository(database),
    )
    config = ResearchJobConfig(
        strategy_name="cm_macd_ult_mtf",
        symbol="XAUUSDT",
        interval="4h",
        start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end=datetime(2026, 1, 4, 20, 0, tzinfo=timezone.utc),
        mode="optimize",
        parameter_space=_auto_fit_parameter_space("cm_macd_ult_mtf"),
    )

    report_grid = service.optimize(config, method="grid")
    report_heuristic = service.optimize(config, method="heuristic")

    assert report_heuristic.best_parameters == report_grid.best_parameters
    assert report_heuristic.metrics["total_return"] == report_grid.metrics["total_return"]
