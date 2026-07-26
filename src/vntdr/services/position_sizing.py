"""ATR stop-distance position sizing, independent of venue contract details."""
from __future__ import annotations

from vntdr.models import PositionSizingDecision


class AtrRiskSizer:
    def __init__(
        self,
        *,
        risk_fraction: float = 0.01,
        stop_atr_multiple: float = 2.0,
        max_notional_fraction: float = 0.30,
    ) -> None:
        if risk_fraction <= 0 or stop_atr_multiple <= 0 or max_notional_fraction <= 0:
            raise ValueError("risk sizing parameters must be positive")
        self.risk_fraction = risk_fraction
        self.stop_atr_multiple = stop_atr_multiple
        self.max_notional_fraction = max_notional_fraction

    def size(self, *, equity: float, price: float, atr: float) -> PositionSizingDecision:
        if equity <= 0 or price <= 0 or atr <= 0:
            return PositionSizingDecision(units=0.0, notional=0.0, risk_budget=0.0, stop_distance=0.0)
        risk_budget = equity * self.risk_fraction
        stop_distance = atr * self.stop_atr_multiple
        raw_units = risk_budget / stop_distance
        max_units = equity * self.max_notional_fraction / price
        units = min(raw_units, max_units)
        return PositionSizingDecision(
            units=units,
            notional=units * price,
            risk_budget=risk_budget,
            stop_distance=stop_distance,
            capped=raw_units > max_units,
        )
