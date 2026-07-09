from __future__ import annotations

from datetime import datetime, timezone
import pytest

from vntdr.config import Settings
from vntdr.models import ResearchJobConfig
from vntdr.services.research import BacktestOutcome, ResearchService
from vntdr.storage.database import Database
from vntdr.storage.repositories import MarketDataRepository, ResearchRunRepository
from vntdr.webapp import _auto_fit_parameter_space


class AlwaysLongStrategy:
    @classmethod
    def signal_for_index(cls, _bars, _index, _parameters):
        return 1


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


def test_return_target_prefers_total_return_over_sharpe(
    tmp_path,
    env_map: dict[str, str],
    sample_xau_bar_payloads: list[dict[str, object]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "research.sqlite3"
    database = Database(f"sqlite+pysqlite:///{db_path}")
    database.create_schema()
    repository = MarketDataRepository(database)
    repository.upsert_bars_from_payloads(sample_xau_bar_payloads)

    settings = Settings.from_mapping(
        {
            **env_map,
            "VNTDR_DATABASE_URL": f"sqlite+pysqlite:///{db_path}",
            "VNTDR_REPORT_DIR": str(tmp_path / "reports"),
        }
    )
    service = ResearchService(
        settings=settings,
        market_data_repository=repository,
        research_run_repository=ResearchRunRepository(database),
    )

    def fake_backtest(_bars, _strategy_name, parameters):
        is_high_return = parameters["fast_length"] == 8
        metrics = {
            "total_return": 0.20 if is_high_return else 0.10,
            "sharpe_ratio": 1.0 if is_high_return else 5.0,
            "max_drawdown": -0.01,
            "trade_count": 2,
        }
        return BacktestOutcome(metrics=metrics, equity_curve=[1.0], signals=[])

    monkeypatch.setattr(service, "_execute_backtest", fake_backtest)

    config = ResearchJobConfig(
        strategy_name="cm_macd_ult_mtf",
        symbol="XAUUSDT",
        interval="4h",
        start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end=datetime(2026, 1, 4, 20, 0, tzinfo=timezone.utc),
        mode="optimize",
        parameter_space={
            "fast_length": [6, 8],
            "slow_length": [20],
            "signal_length": [9],
            "trend_window": [9],
        },
        optimize_target="return",
    )

    report = service.optimize(config, method="heuristic")

    assert report.best_parameters["fast_length"] == 8
    assert report.metrics["total_return"] == 0.20


def test_heuristic_uses_exact_search_for_medium_sized_spaces(
    tmp_path,
    env_map: dict[str, str],
    sample_xau_bar_payloads: list[dict[str, object]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "research.sqlite3"
    database = Database(f"sqlite+pysqlite:///{db_path}")
    database.create_schema()
    repository = MarketDataRepository(database)
    repository.upsert_bars_from_payloads(sample_xau_bar_payloads)

    settings = Settings.from_mapping(
        {
            **env_map,
            "VNTDR_DATABASE_URL": f"sqlite+pysqlite:///{db_path}",
            "VNTDR_REPORT_DIR": str(tmp_path / "reports"),
        }
    )
    service = ResearchService(
        settings=settings,
        market_data_repository=repository,
        research_run_repository=ResearchRunRepository(database),
    )
    seen: list[dict[str, int]] = []

    def fake_backtest(_bars, _strategy_name, parameters):
        seen.append(parameters.copy())
        total_return = 1.0 if parameters == {"a": 10, "b": 10, "c": 10, "d": 0} else 0.0
        metrics = {
            "total_return": total_return,
            "sharpe_ratio": total_return,
            "max_drawdown": 0.0,
            "trade_count": 1,
        }
        return BacktestOutcome(metrics=metrics, equity_curve=[1.0], signals=[])

    monkeypatch.setattr(service, "_execute_backtest", fake_backtest)

    evaluations = service._evaluate_parameter_space(
        bars=repository.fetch_bars(
            "XAUUSDT",
            "4h",
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 4, 20, 0, tzinfo=timezone.utc),
        ),
        strategy_name="cm_macd_ult_mtf",
        parameter_space={
            "a": list(range(11)),
            "b": list(range(11)),
            "c": list(range(11)),
            "d": [0],
        },
        method="heuristic",
        optimize_target="return",
    )

    assert len(seen) == 11 * 11 * 11
    assert evaluations[0][0] == {"a": 10, "b": 10, "c": 10, "d": 0}


def test_backtest_fee_rate_affects_total_return_and_step_metrics(
    tmp_path,
    env_map: dict[str, str],
    sample_xau_bar_payloads: list[dict[str, object]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "research.sqlite3"
    database = Database(f"sqlite+pysqlite:///{db_path}")
    database.create_schema()
    repository = MarketDataRepository(database)
    repository.upsert_bars_from_payloads(sample_xau_bar_payloads[:3])

    settings = Settings.from_mapping(
        {
            **env_map,
            "VNTDR_DATABASE_URL": f"sqlite+pysqlite:///{db_path}",
            "VNTDR_REPORT_DIR": str(tmp_path / "reports"),
            "VNTDR_TAKER_FEE_RATE": "0.01",
            "VNTDR_USE_MAKER_FEE": "false",
        }
    )
    service = ResearchService(
        settings=settings,
        market_data_repository=repository,
        research_run_repository=ResearchRunRepository(database),
    )
    monkeypatch.setattr(service, "_load_strategy", lambda _strategy_name: AlwaysLongStrategy)

    bars = repository.fetch_bars(
        "XAUUSDT",
        "4h",
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc),
    )
    outcome = service._execute_backtest(bars, "always_long", {})

    gross_return = bars[-1].close / bars[0].close - 1
    assert outcome.metrics["total_return"] < gross_return
    assert outcome.metrics["trade_count"] == 2.0
    assert outcome.metrics["win_rate"] < 1.0
