"""Small, dependency-free indicator helpers shared by built-in strategies."""

from __future__ import annotations

from vntdr.models import BarRecord


def bars_fingerprint(bars: list[BarRecord]) -> tuple[object, ...]:
    """Return a cheap fingerprint so cached indicators cannot leak across runs."""

    if not bars:
        return (0,)
    first, last = bars[0], bars[-1]
    return (
        len(bars),
        first.datetime,
        first.open,
        first.high,
        first.low,
        first.close,
        first.volume,
        last.datetime,
        last.open,
        last.high,
        last.low,
        last.close,
        last.volume,
    )


def rolling_mean(values: list[float], window: int, index: int, *, include_current: bool = True) -> float:
    end = index + 1 if include_current else index
    start = max(0, end - window)
    sample = values[start:end]
    return sum(sample) / len(sample) if sample else 0.0


def rsi_series(closes: list[float], period: int) -> list[float | None]:
    """Wilder RSI with ``None`` during the warm-up period."""

    period = max(1, int(period))
    result: list[float | None] = [None] * len(closes)
    if len(closes) <= period:
        return result

    gains = [max(closes[i] - closes[i - 1], 0.0) for i in range(1, len(closes))]
    losses = [max(closes[i - 1] - closes[i], 0.0) for i in range(1, len(closes))]
    average_gain = sum(gains[:period]) / period
    average_loss = sum(losses[:period]) / period

    def value() -> float:
        if average_loss == 0:
            return 100.0 if average_gain > 0 else 50.0
        relative_strength = average_gain / average_loss
        return 100.0 - 100.0 / (1.0 + relative_strength)

    result[period] = value()
    for index in range(period + 1, len(closes)):
        average_gain = (average_gain * (period - 1) + gains[index - 1]) / period
        average_loss = (average_loss * (period - 1) + losses[index - 1]) / period
        result[index] = value()
    return result


def kdj_series(
    bars: list[BarRecord],
    k_period: int,
    d_period: int,
    j_period: int,
) -> tuple[list[float | None], list[float | None], list[float | None]]:
    """Return K, D and J values using the common RSV/KDJ formulation."""

    k_period = max(1, int(k_period))
    d_period = max(1, int(d_period))
    j_period = max(1, int(j_period))
    k_values: list[float | None] = [None] * len(bars)
    d_values: list[float | None] = [None] * len(bars)
    j_values: list[float | None] = [None] * len(bars)
    k_value, d_value = 50.0, 50.0

    for index in range(k_period - 1, len(bars)):
        window = bars[index - k_period + 1 : index + 1]
        highest = max(bar.high for bar in window)
        lowest = min(bar.low for bar in window)
        rsv = 50.0 if highest == lowest else (bars[index].close - lowest) / (highest - lowest) * 100.0
        k_value = ((d_period - 1) * k_value + rsv) / d_period
        d_value = ((j_period - 1) * d_value + k_value) / j_period
        k_values[index] = k_value
        d_values[index] = d_value
        j_values[index] = 3.0 * k_value - 2.0 * d_value

    return k_values, d_values, j_values
