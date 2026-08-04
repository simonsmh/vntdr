from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from vntdr.config import Settings
from vntdr.models import BarRecord
from vntdr.services.config_service import ConfigService
from vntdr.services.research import BacktestOutcome


def _bars() -> list[BarRecord]:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return [
        BarRecord(
            symbol="XAU-USDT-SWAP",
            exchange="OKX",
            interval="4h",
            datetime=start + timedelta(hours=4 * index),
            open=100 + index,
            high=102 + index,
            low=99 + index,
            close=101 + index,
            volume=100 + index,
        )
        for index in range(4)
    ]


def test_strategy_change_registers_chart_preview_callback(tmp_path, env_map, monkeypatch) -> None:
    import vntdr.webapp as webapp

    settings = Settings.from_mapping({
        **env_map,
        "VNTDR_DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'responsive.sqlite'}",
    })
    config_service = ConfigService(settings, config_file=Path(tmp_path / "config_override.json"))
    captured: dict[str, object] = {}
    monkeypatch.setattr(webapp, "_get_config_service", lambda: config_service)
    monkeypatch.setattr(
        webapp.gr.Blocks,
        "launch",
        lambda app, **_: captured.setdefault("app", app),
    )

    webapp.main(port=7860)
    app = captured["app"]
    preview_functions = [
        block_function.fn
        for block_function in app.fns.values()
        if block_function.fn.__qualname__.endswith("preview_strategy_chart")
    ]

    assert len(preview_functions) == 8

    class FakeResearch:
        def _load_bars(self, _config):
            return _bars()

        def _execute_backtest(self, _bars, _strategy_name, _parameters):
            return BacktestOutcome(
                metrics={},
                equity_curve=[1.0],
                signals=[0, 1, 1, 0],
            )

    monkeypatch.setattr(webapp, "_get_services", lambda: (FakeResearch(), None, None))
    chart, status = preview_functions[0](
        "kdj",
        "XAU-USDT-SWAP",
        "4h",
        datetime(2026, 1, 1),
        datetime(2026, 1, 2),
        "both",
        "",
        "k_period=3\nd_period=3\nj_period=3",
    )

    assert "KDJ指标" in chart.layout.title.text
    assert "自动刷新" in status


def test_preview_version_rejects_outdated_chart_requests() -> None:
    import vntdr.webapp as webapp

    first = webapp._start_preview()
    second = webapp._start_preview()

    assert not webapp._is_current_preview(first)
    assert webapp._is_current_preview(second)


def test_etf_timing_candidate_has_reference_price_and_observation_range() -> None:
    import vntdr.webapp as webapp

    rows = pd.DataFrame(
        [
            {
                "trade_date": datetime(2026, 7, 27) + timedelta(days=index),
                "symbol": "510300",
                "main_net_inflow": 100_000_000 + index * 10_000_000,
                "close_price": 4.60 + index * 0.01,
                "open_price": 4.59 + index * 0.01,
                "volume": 1000 + index * 10,
            }
            for index in range(5)
        ]
    )

    result = webapp._etf_timing_candidates(
        rows,
        symbol="510300",
        name_map={"510300": "沪深300ETF"},
    )

    assert not result.empty
    assert result.iloc[0]["估算参考价"] > 0
    assert "–" in result.iloc[0]["估算观察区间"]
    assert result.iloc[0]["估算时机"].startswith("下一交易日开盘后")

    overview = webapp._etf_timing_candidates(rows, name_map={"510300": "沪深300ETF"})
    assert list(overview["标的代码"]) == ["510300"]


def test_etf_exit_candidate_marks_sell_or_reduction_observation() -> None:
    import vntdr.webapp as webapp

    rows = pd.DataFrame(
        [
            {
                "trade_date": datetime(2026, 7, 27) + timedelta(days=index),
                "symbol": "510300",
                "main_net_inflow": -100_000_000 - index * 10_000_000,
                "close_price": 4.70 - index * 0.03,
                "open_price": 4.71 - index * 0.03,
                "volume": 1000 + index * 120,
            }
            for index in range(5)
        ]
    )

    result = webapp._etf_exit_candidates(
        rows,
        symbol="510300",
        name_map={"510300": "沪深300ETF"},
    )

    assert not result.empty
    assert result.iloc[0]["评分"] >= 60
    assert "卖出/减仓" in result.iloc[0]["估算时机"]
    assert "主力净流出" in result.iloc[0]["依据"]
