"""KDJ stochastic oscillator strategy."""

from __future__ import annotations

from typing import Any

from vntdr.models import BarRecord
from vntdr.strategies.base import ReviewedStrategyBase
from vntdr.strategies.indicators import bars_fingerprint, kdj_series

STRATEGY_LABEL = "KDJ 随机指标"
STRATEGY_DESCRIPTION = "超卖金叉做多、超买死叉做空，反向交叉或极值退出。"

DEFAULT_PARAMETERS = {
    "k_period": 9,
    "d_period": 3,
    "j_period": 3,
    "oversold": 20.0,
    "overbought": 80.0,
}

DEFAULT_PARAMETER_SPACE = {
    "k_period": [5, 9, 14],
    "d_period": [3, 5],
    "j_period": [3, 5],
    "oversold": [15.0, 20.0, 25.0],
    "overbought": [75.0, 80.0, 85.0],
}

DEFAULT_PARAMETER_BOUNDS = {
    "k_period": "3~30",
    "d_period": "2~15",
    "j_period": "2~15",
    "oversold": "5~40:5",
    "overbought": "60~95:5",
}


class Strategy(ReviewedStrategyBase):
    """A stateful target-position strategy based on K/D crossovers."""

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
        k_values, d_values, j_values = kdj_series(
            bars, int(p["k_period"]), int(p["d_period"]), int(p["j_period"])
        )
        oversold, overbought = float(p["oversold"]), float(p["overbought"])
        signals = [0] * len(bars)
        position = 0
        for index in range(1, len(bars)):
            k, d, j = k_values[index], d_values[index], j_values[index]
            previous_k, previous_d = k_values[index - 1], d_values[index - 1]
            if None in (k, d, j, previous_k, previous_d):
                continue
            cross_up = previous_k <= previous_d and k > d
            cross_down = previous_k >= previous_d and k < d
            if position == 0:
                if cross_up and (previous_k <= oversold or j <= oversold):
                    position = 1
                elif cross_down and (previous_k >= overbought or j >= overbought):
                    position = -1
            elif position == 1 and (cross_down or j >= overbought):
                position = 0
            elif position == -1 and (cross_up or j <= oversold):
                position = 0
            signals[index] = position
        return signals
