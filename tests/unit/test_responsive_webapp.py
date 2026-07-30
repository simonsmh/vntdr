from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

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
