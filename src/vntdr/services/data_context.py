"""Point-in-time market data access for strategies and factor plugins."""
from __future__ import annotations

from datetime import datetime

from vntdr.models import BarRecord, FactorObservation, Interval


class MarketDataContext:
    """Only exposes bars closed by a requested decision timestamp.

    Strategies receive this object rather than a provider directly so a 4h
    decision cannot accidentally read an unfinished 1d candle.
    """

    def __init__(
        self,
        bars_by_interval: dict[str, list[BarRecord]],
        factors: list[FactorObservation] | None = None,
    ) -> None:
        self._bars = {Interval(value=key).value: sorted(values, key=lambda bar: bar.datetime) for key, values in bars_by_interval.items()}
        self._factors = sorted(factors or [], key=lambda item: (item.available_at, item.observed_at))

    def closed_bars(self, interval: str, at: datetime) -> list[BarRecord]:
        normalized = Interval(value=interval)
        return [bar for bar in self._bars.get(normalized.value, []) if bar.datetime.timestamp() + normalized.seconds <= at.timestamp()]

    def latest_closed_bar(self, interval: str, at: datetime) -> BarRecord | None:
        bars = self.closed_bars(interval, at)
        return bars[-1] if bars else None

    def coverage(self, interval: str, at: datetime, minimum_bars: int) -> bool:
        return len(self.closed_bars(interval, at)) >= minimum_bars

    def available_factors(self, factor_name: str, at: datetime) -> list[FactorObservation]:
        return [
            factor for factor in self._factors
            if factor.factor_name == factor_name
            and factor.observed_at <= at
            and factor.available_at <= at
        ]

    def latest_factor(self, factor_name: str, at: datetime) -> FactorObservation | None:
        factors = self.available_factors(factor_name, at)
        return factors[-1] if factors else None
