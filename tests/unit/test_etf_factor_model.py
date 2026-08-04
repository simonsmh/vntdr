from __future__ import annotations

from datetime import date, timedelta
from math import isclose

import pandas as pd

from vntdr.services.etf_factor_model import (
    EtfFactorModelConfig,
    build_etf_factor_frame,
    run_etf_factor_model,
)


def _rows(symbols: int = 12, days: int = 55) -> list[dict[str, object]]:
    values: list[dict[str, object]] = []
    start = date(2026, 1, 1)
    for symbol_index in range(symbols):
        base = 4.0 + symbol_index * 0.01
        for day_index in range(days):
            close = base + day_index * 0.002 + (symbol_index % 3) * 0.001
            open_price = close - 0.001
            flow = (symbol_index - 5) * 10_000_000 + day_index * 100_000
            values.append(
                {
                    "symbol": f"{510300 + symbol_index:06d}",
                    "trade_date": start + timedelta(days=day_index),
                    "open_price": open_price,
                    "high_price": close + 0.01,
                    "low_price": open_price - 0.01,
                    "close_price": close,
                    "volume": 1_000_000 + day_index * 1_000,
                    "main_net_inflow": flow,
                    "main_inflow_ratio": flow / 10_000_000,
                    "extra_large_net_inflow": flow * 0.7,
                    "large_net_inflow": flow * 0.3,
                    "large_inflow_ratio": flow / 20_000_000,
                }
            )
    return values


def test_factor_frame_uses_next_open_and_future_close_for_label() -> None:
    frame = build_etf_factor_frame(_rows(symbols=2, days=8), horizon_days=3)
    row = frame[
        (frame["symbol"] == "510300")
        & (frame["trade_date"].dt.date == date(2026, 1, 1))
    ].iloc[0]

    expected_next_open = 4.001
    expected_exit_close = 4.006
    assert isclose(row["next_open"], expected_next_open)
    assert isclose(row["exit_close"], expected_exit_close)
    assert isclose(row["forward_return"], (expected_exit_close / expected_next_open) - 1)
    assert pd.isna(frame.iloc[-1]["forward_return"])


def test_factor_model_returns_walk_forward_scores_and_metrics() -> None:
    result = run_etf_factor_model(
        _rows(),
        config=EtfFactorModelConfig(
            horizon_days=3,
            min_train_days=25,
            test_days=8,
            step_days=8,
            top_k=4,
        ),
    )

    assert result.status == "ok"
    assert not result.latest_scores.empty
    assert not result.fold_metrics.empty
    assert not result.event_returns.empty
    assert {"model_score", "trend", "rank"}.issubset(result.latest_scores.columns)
    assert result.metrics["event_count"] == len(result.event_returns)
    assert "warnings" not in result.metrics
