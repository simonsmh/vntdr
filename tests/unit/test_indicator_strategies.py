from __future__ import annotations

from datetime import datetime, timedelta, timezone

from vntdr.config import Settings
from vntdr.models import BarRecord
from vntdr.services.config_service import ConfigService
from vntdr.strategies.kdj import Strategy as KdjStrategy
from vntdr.strategies.registry import available_strategy_names
from vntdr.strategies.rsi import Strategy as RsiStrategy
from vntdr.strategies.volume import Strategy as VolumeStrategy


def _oscillating_bars() -> list[BarRecord]:
    closes = [
        100, 99, 98, 97, 96, 95, 94, 93, 94, 96, 99, 102, 104, 105,
        104, 102, 99, 96, 94, 93, 94, 97, 101, 105, 108, 109, 107, 104,
        100, 96, 93, 92, 94, 98, 103, 107, 110, 109, 106, 102, 98, 95,
    ]
    bars = []
    for index, close in enumerate(closes):
        bars.append(BarRecord(
            symbol="XAU-USDT-SWAP", exchange="OKX", interval="4h",
            datetime=datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(hours=4 * index),
            open=close - 0.5, high=close + 2, low=close - 2, close=close,
            volume=100 + (80 if index in {10, 23, 34} else 0),
        ))
    return bars


def test_registry_discovers_oscillator_and_volume_strategies() -> None:
    names = available_strategy_names()
    assert names[:4] == ["cm_macd_ult_mtf", "kdj", "rsi", "volume"]


def test_kdj_and_rsi_emit_both_directions() -> None:
    bars = _oscillating_bars()
    kdj_signals = [KdjStrategy.signal_for_index(bars, i, {
        "k_period": 3, "d_period": 3, "j_period": 3,
        "oversold": 30, "overbought": 70,
    }) for i in range(len(bars))]
    rsi_signals = [RsiStrategy.signal_for_index(bars, i, {
        "rsi_period": 3, "oversold": 35, "overbought": 65,
        "exit_midline": 50,
    }) for i in range(len(bars))]

    assert {1, -1}.issubset(set(kdj_signals))
    assert {1, -1}.issubset(set(rsi_signals))


def test_volume_strategy_requires_volume_confirmation_for_breakout() -> None:
    bars = _oscillating_bars()
    signals = [VolumeStrategy.signal_for_index(bars, i, {
        "volume_window": 3, "volume_multiplier": 1.2, "price_window": 3,
    }) for i in range(len(bars))]

    assert 1 in signals
    assert -1 not in signals
    assert all(signal in {-1, 0, 1} for signal in signals)


def test_enabled_strategy_catalog_is_persisted(tmp_path, env_map) -> None:
    settings = Settings.from_mapping({**env_map, "VNTDR_DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'config.sqlite'}"})
    service = ConfigService(settings, config_file=tmp_path / "config_override.json")

    assert service.set("research.enabled_strategies", ["kdj", "rsi"])
    assert service.get("research.enabled_strategies") == ["kdj", "rsi"]

    reloaded = ConfigService(Settings.from_mapping({**env_map, "VNTDR_DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'config2.sqlite'}"}), config_file=tmp_path / "config_override.json")
    assert reloaded.get("research.enabled_strategies") == ["kdj", "rsi"]
