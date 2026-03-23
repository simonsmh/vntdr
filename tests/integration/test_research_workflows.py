from __future__ import annotations

from datetime import datetime, timezone

from vntdr.config import Settings
from vntdr.models import ResearchJobConfig
from vntdr.services.research import ResearchService
from vntdr.storage.database import Database
from vntdr.storage.repositories import MarketDataRepository, ResearchRunRepository


def test_backtest_optimize_and_walk_forward_generate_reports(
    tmp_path,
    env_map: dict[str, str],
    sample_bar_payloads: list[dict[str, object]],
) -> None:
    db_path = tmp_path / "research.sqlite3"
    report_dir = tmp_path / "reports"
    database = Database(f"sqlite+pysqlite:///{db_path}")
    database.create_schema()
    repository = MarketDataRepository(database)
    repository.upsert_bars_from_payloads(sample_bar_payloads)

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

    backtest_config = ResearchJobConfig(
        strategy_name="demo_momentum",
        symbol="BTC-USDT-SWAP",
        interval="1m",
        start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end=datetime(2026, 1, 1, 0, 9, tzinfo=timezone.utc),
        parameters={"lookback": 3},
    )
    backtest = service.backtest(backtest_config)
    optimize = service.optimize(
        backtest_config.model_copy(
            update={
                "mode": "optimize",
                "parameter_space": {"lookback": [2, 3, 4]},
            }
        )
    )
    walk_forward = service.walk_forward(
        backtest_config.model_copy(
            update={
                "mode": "walk-forward",
                "parameter_space": {"lookback": [2, 3, 4]},
                "train_window": 5,
                "test_window": 3,
            }
        )
    )

    assert backtest.metrics["trade_count"] >= 1
    assert optimize.best_parameters["lookback"] in {2, 3, 4}
    assert len(optimize.top_results) == 3
    assert len(walk_forward.fold_results) >= 1
    assert report_dir.joinpath("demo_momentum_backtest.md").exists()
    assert report_dir.joinpath("demo_momentum_optimize.json").exists()
    assert report_dir.joinpath("demo_momentum_walk_forward.md").exists()


def test_cm_macd_strategy_can_optimize_xauusdt(
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

    report = service.optimize(config)

    assert set(report.best_parameters) == {"fast_length", "slow_length", "signal_length", "trend_window"}
    assert report.metrics["trade_count"] >= 1
    assert report_dir.joinpath("cm_macd_ult_mtf_optimize.md").exists()
