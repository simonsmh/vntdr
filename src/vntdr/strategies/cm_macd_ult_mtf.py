from __future__ import annotations

from typing import Any

from vntdr.models import BarRecord
from vntdr.strategies.base import ReviewedStrategyBase

DEFAULT_PARAMETERS = {
    "fast_length": 4,
    "slow_length": 8,
    "signal_length": 3,
    "trend_window": 3,
}

DEFAULT_PARAMETER_SPACE = {
    "fast_length": [3, 4, 5],
    "slow_length": [6, 7, 8, 9],
    "signal_length": [3, 4],
    "trend_window": [2, 3, 4],
}


def _ema(values: list[float], length: int) -> list[float]:
    alpha = 2.0 / (length + 1)
    ema_values = [values[0]]
    for value in values[1:]:
        ema_values.append(alpha * value + (1 - alpha) * ema_values[-1])
    return ema_values


class Strategy(ReviewedStrategyBase):
    """A lightweight CM_MacD_Ult_MTF-inspired multi-timeframe momentum strategy."""

    @classmethod
    def signal_for_index(
        cls,
        bars: list[BarRecord],
        index: int,
        parameters: dict[str, Any],
    ) -> int:
        defaults = {**DEFAULT_PARAMETERS, **parameters}
        fast_length = int(defaults["fast_length"])
        slow_length = int(defaults["slow_length"])
        signal_length = int(defaults["signal_length"])
        trend_window = int(defaults["trend_window"])
        if fast_length >= slow_length or index < slow_length:
            return 0

        closes = [bar.close for bar in bars[: index + 1]]
        fast_ema = _ema(closes, fast_length)
        slow_ema = _ema(closes, slow_length)
        macd_line = [fast - slow for fast, slow in zip(fast_ema, slow_ema, strict=True)]
        signal_line = _ema(macd_line, signal_length)
        histogram = [macd - signal for macd, signal in zip(macd_line, signal_line, strict=True)]
        if len(histogram) < trend_window:
            return 0
        trend_histogram = histogram[-trend_window:]
        current_histogram = histogram[-1]
        higher_trend = sum(trend_histogram) / len(trend_histogram)
        price_bias = closes[-1] - slow_ema[-1]
        if current_histogram > 0 and higher_trend > 0 and price_bias >= 0:
            return 1
        if current_histogram < 0 and higher_trend < 0 and price_bias <= 0:
            return -1
        return 0
