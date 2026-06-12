from __future__ import annotations

from typing import Any

from vntdr.models import BarRecord
from vntdr.strategies.base import ReviewedStrategyBase

DEFAULT_PARAMETERS = {
    "fast_length": 6,
    "slow_length": 21,
    "signal_length": 3,
    "trend_window": 7,
}

DEFAULT_PARAMETER_SPACE = {
    "fast_length": [2, 4, 6, 8, 10, 12],
    "slow_length": [10, 15, 20, 25, 30],
    "signal_length": [3, 5, 7, 9],
    "trend_window": [3, 5, 7, 9],
}


def _ema(values: list[float], length: int) -> list[float]:
    alpha = 2.0 / (length + 1)
    ema_values = [values[0]]
    for value in values[1:]:
        ema_values.append(alpha * value + (1 - alpha) * ema_values[-1])
    return ema_values


class Strategy(ReviewedStrategyBase):
    """A lightweight CM_MacD_Ult_MTF-inspired multi-timeframe momentum strategy."""

    # Thread-safe/run-safe class cache to store precomputed signals.
    # Cache key: (id(bars), tuple(sorted(parameters.items()))) -> list of signals
    _cache: dict[tuple[int, tuple[tuple[str, Any], ...]], list[int]] = {}

    @classmethod
    def signal_for_index(
        cls,
        bars: list[BarRecord],
        index: int,
        parameters: dict[str, Any],
    ) -> int:
        defaults = {**DEFAULT_PARAMETERS, **parameters}
        
        cache_key = (id(bars), tuple(sorted(defaults.items())))
        if cache_key not in cls._cache:
            cls._cache[cache_key] = cls._precompute_signals(bars, defaults)
            
        return cls._cache[cache_key][index]

    @classmethod
    def _precompute_signals(cls, bars: list[BarRecord], defaults: dict[str, Any]) -> list[int]:
        fast_length = int(defaults["fast_length"])
        slow_length = int(defaults["slow_length"])
        signal_length = int(defaults["signal_length"])
        trend_window = int(defaults["trend_window"])
        
        signals = [0] * len(bars)
        if fast_length >= slow_length or len(bars) <= slow_length:
            return signals

        # 1. Precompute EMAs for the entire series in O(N)
        closes = [bar.close for bar in bars]
        
        # fast_ema
        alpha_fast = 2.0 / (fast_length + 1)
        fast_ema = [closes[0]]
        for val in closes[1:]:
            fast_ema.append(alpha_fast * val + (1 - alpha_fast) * fast_ema[-1])
            
        # slow_ema
        alpha_slow = 2.0 / (slow_length + 1)
        slow_ema = [closes[0]]
        for val in closes[1:]:
            slow_ema.append(alpha_slow * val + (1 - alpha_slow) * slow_ema[-1])
            
        # macd_line
        macd_line = [f - s for f, s in zip(fast_ema, slow_ema, strict=True)]
        
        # signal_line
        alpha_sig = 2.0 / (signal_length + 1)
        signal_line = [macd_line[0]]
        for val in macd_line[1:]:
            signal_line.append(alpha_sig * val + (1 - alpha_sig) * signal_line[-1])
            
        # histogram
        histogram = [m - s for m, s in zip(macd_line, signal_line, strict=True)]
        
        # 2. Compute signals for all indexes
        for index in range(slow_length, len(bars)):
            if index < trend_window - 1:
                continue
            
            trend_histogram = histogram[index - trend_window + 1 : index + 1]
            higher_trend = sum(trend_histogram) / trend_window
            current_histogram = histogram[index]
            price_bias = closes[index] - slow_ema[index]
            
            if current_histogram > 0 and higher_trend > 0 and price_bias >= 0:
                signals[index] = 1
            elif current_histogram < 0 and higher_trend < 0 and price_bias <= 0:
                signals[index] = -1
            else:
                signals[index] = 0
                
        return signals

