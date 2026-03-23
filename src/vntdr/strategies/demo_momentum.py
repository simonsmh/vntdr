from __future__ import annotations

from statistics import mean
from typing import Any

from vntdr.models import BarRecord
from vntdr.strategies.base import ReviewedStrategyBase


class Strategy(ReviewedStrategyBase):
    """Simple reviewed momentum strategy used for tests and local validation."""

    @classmethod
    def signal_for_index(
        cls,
        bars: list[BarRecord],
        index: int,
        parameters: dict[str, Any],
    ) -> int:
        lookback = int(parameters.get("lookback", 3))
        if index < lookback:
            return 0
        window = [bar.close for bar in bars[index - lookback : index]]
        return 1 if bars[index].close >= mean(window) else 0
