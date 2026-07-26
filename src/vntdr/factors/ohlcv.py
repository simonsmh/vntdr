from __future__ import annotations

from dataclasses import dataclass

from vntdr.factors.base import FactorPlugin
from vntdr.models import BarRecord


@dataclass(frozen=True)
class TrendFactor:
    window: int = 20
    name: str = "trend_return"

    def compute(self, bars: list[BarRecord], index: int) -> float | None:
        if index < self.window or bars[index - self.window].close == 0:
            return None
        return bars[index].close / bars[index - self.window].close - 1


@dataclass(frozen=True)
class BreakoutFactor:
    window: int = 20
    name: str = "breakout_position"

    def compute(self, bars: list[BarRecord], index: int) -> float | None:
        if index < self.window:
            return None
        prior = bars[index - self.window:index]
        high, low = max(bar.high for bar in prior), min(bar.low for bar in prior)
        if high == low:
            return 0.0
        return 2 * ((bars[index].close - low) / (high - low)) - 1


@dataclass(frozen=True)
class AtrRatioFactor:
    window: int = 14
    name: str = "atr_ratio"

    def compute(self, bars: list[BarRecord], index: int) -> float | None:
        if index < max(1, self.window) or bars[index].close == 0:
            return None
        ranges = []
        for offset in range(index - self.window + 1, index + 1):
            bar, previous = bars[offset], bars[offset - 1]
            ranges.append(max(bar.high - bar.low, abs(bar.high - previous.close), abs(bar.low - previous.close)))
        return sum(ranges) / len(ranges) / bars[index].close
