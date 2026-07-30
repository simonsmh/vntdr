"""Volume-confirmed breakout strategy."""

from __future__ import annotations

from typing import Any

from vntdr.models import BarRecord
from vntdr.strategies.base import ReviewedStrategyBase
from vntdr.strategies.indicators import bars_fingerprint, rolling_mean

STRATEGY_LABEL = "成交量突破"
STRATEGY_DESCRIPTION = "放量突破前高/前低时建立方向仓位，反向突破时反转。"

DEFAULT_PARAMETERS = {
    "volume_window": 20,
    "volume_multiplier": 1.5,
    "price_window": 20,
}

DEFAULT_PARAMETER_SPACE = {
    "volume_window": [10, 20, 30],
    "volume_multiplier": [1.2, 1.5, 2.0],
    "price_window": [10, 20, 30],
}

DEFAULT_PARAMETER_BOUNDS = {
    "volume_window": "3~80",
    "volume_multiplier": "1~3:0.25",
    "price_window": "3~80",
}


class Strategy(ReviewedStrategyBase):
    _cache: dict[tuple[int, tuple[tuple[str, Any], ...]], tuple[tuple[object, ...], list[int]]] = {}

    @classmethod
    def signal_for_index(cls, bars: list[BarRecord], index: int, parameters: dict[str, Any]) -> int:
        p = {**DEFAULT_PARAMETERS, **parameters}
        key = (id(bars), tuple(sorted(p.items())))
        fingerprint = bars_fingerprint(bars)
        cached = cls._cache.get(key)
        if cached is None or cached[0] != fingerprint:
            cls._cache[key] = (fingerprint, cls._precompute_signals(bars, p))
        return cls._cache[key][1][index]

    @classmethod
    def _precompute_signals(cls, bars: list[BarRecord], p: dict[str, Any]) -> list[int]:
        volume_window = max(1, int(p["volume_window"]))
        price_window = max(1, int(p["price_window"]))
        multiplier = float(p["volume_multiplier"])
        signals = [0] * len(bars)
        position = 0
        volumes = [max(0.0, bar.volume) for bar in bars]
        closes = [bar.close for bar in bars]
        warmup = max(volume_window, price_window)
        for index in range(warmup, len(bars)):
            average_volume = rolling_mean(volumes, volume_window, index, include_current=False)
            previous_closes = closes[index - price_window : index]
            if not previous_closes or average_volume <= 0:
                continue
            high, low = max(previous_closes), min(previous_closes)
            high_volume = volumes[index] >= average_volume * multiplier
            if high_volume and closes[index] > high:
                position = 1
            elif high_volume and closes[index] < low:
                position = -1
            elif position == 1 and closes[index] < low:
                position = 0
            elif position == -1 and closes[index] > high:
                position = 0
            signals[index] = position
        return signals
