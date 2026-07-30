"""RSI mean-reversion strategy."""

from __future__ import annotations

from typing import Any

from vntdr.models import BarRecord
from vntdr.strategies.base import ReviewedStrategyBase
from vntdr.strategies.indicators import bars_fingerprint, rsi_series

STRATEGY_LABEL = "RSI 相对强弱"
STRATEGY_DESCRIPTION = "RSI 从超卖区回升做多、从超买区回落做空，并在中轴/极值退出。"

DEFAULT_PARAMETERS = {
    "rsi_period": 14,
    "oversold": 30.0,
    "overbought": 70.0,
    "exit_midline": 50.0,
}

DEFAULT_PARAMETER_SPACE = {
    "rsi_period": [7, 14, 21],
    "oversold": [20.0, 30.0, 35.0],
    "overbought": [65.0, 70.0, 80.0],
    "exit_midline": [45.0, 50.0, 55.0],
}

DEFAULT_PARAMETER_BOUNDS = {
    "rsi_period": "2~50",
    "oversold": "5~45:5",
    "overbought": "55~95:5",
    "exit_midline": "35~65:5",
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
        values = rsi_series([bar.close for bar in bars], int(p["rsi_period"]))
        oversold, overbought = float(p["oversold"]), float(p["overbought"])
        exit_midline = float(p["exit_midline"])
        signals = [0] * len(bars)
        position = 0
        for index in range(1, len(values)):
            current, previous = values[index], values[index - 1]
            if current is None or previous is None:
                continue
            crossed_up_from_oversold = previous <= oversold < current
            crossed_down_from_overbought = previous >= overbought > current
            if position == 0:
                if crossed_up_from_oversold:
                    position = 1
                elif crossed_down_from_overbought:
                    position = -1
            elif position == 1 and (current >= overbought or (previous >= exit_midline > current)):
                position = 0
            elif position == -1 and (current <= oversold or (previous <= exit_midline < current)):
                position = 0
            signals[index] = position
        return signals
