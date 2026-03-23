from __future__ import annotations

from datetime import datetime, timezone

import pytest

from vntdr.models import FoldResult, ResearchJobConfig, ResearchReport


def test_research_job_config_rejects_invalid_dates() -> None:
    with pytest.raises(ValueError):
        ResearchJobConfig(
            strategy_name="demo_momentum",
            symbol="BTC-USDT-SWAP",
            interval="1m",
            start=datetime(2026, 1, 10, tzinfo=timezone.utc),
            end=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )


def test_research_job_config_requires_parameter_space_for_optimization() -> None:
    with pytest.raises(ValueError):
        ResearchJobConfig(
            strategy_name="demo_momentum",
            symbol="BTC-USDT-SWAP",
            interval="1m",
            start=datetime(2026, 1, 1, tzinfo=timezone.utc),
            end=datetime(2026, 1, 10, tzinfo=timezone.utc),
            mode="optimize",
            parameter_space={},
        )


def test_research_report_renders_markdown() -> None:
    report = ResearchReport(
        strategy_name="demo_momentum",
        symbol="BTC-USDT-SWAP",
        interval="1m",
        mode="walk-forward",
        metrics={
            "total_return": 0.12,
            "sharpe_ratio": 1.8,
            "max_drawdown": -0.03,
            "trade_count": 9,
        },
        best_parameters={"lookback": 3},
        fold_results=[
            FoldResult(
                fold_index=1,
                train_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
                train_end=datetime(2026, 1, 10, tzinfo=timezone.utc),
                test_start=datetime(2026, 1, 11, tzinfo=timezone.utc),
                test_end=datetime(2026, 1, 15, tzinfo=timezone.utc),
                metrics={"total_return": 0.05, "sharpe_ratio": 1.4, "max_drawdown": -0.01},
                parameters={"lookback": 3},
            )
        ],
        top_results=[{"lookback": 3, "score": 1.8}],
    )

    markdown = report.to_markdown()

    assert "Sharpe Ratio" in markdown
    assert "demo_momentum" in markdown
    assert "lookback" in markdown
