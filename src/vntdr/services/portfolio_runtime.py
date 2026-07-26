"""Run enabled strategy instances and produce one portfolio-level target."""
from __future__ import annotations

import math
from statistics import StatisticsError, correlation, stdev

from vntdr.models import PortfolioRunResult, StrategyDecision
from vntdr.services.portfolio import PortfolioAllocator
from vntdr.services.strategy_runtime import StrategyRuntimeService
from vntdr.storage.repositories import StrategyRepository


class PortfolioRuntimeService:
    def __init__(
        self,
        *,
        strategy_repository: StrategyRepository,
        strategy_runtime: StrategyRuntimeService,
        monitoring_service,
        allocator: PortfolioAllocator | None = None,
    ) -> None:
        self.strategy_repository = strategy_repository
        self.strategy_runtime = strategy_runtime
        self.monitoring_service = monitoring_service
        self.allocator = allocator or PortfolioAllocator()

    def run_enabled(self, *, volume: float, lookback_bars: int = 120) -> PortfolioRunResult:
        decisions: list[StrategyDecision] = []
        errors: dict[str, str] = {}
        for instance in self.strategy_repository.list_instances(enabled_only=True):
            try:
                result = self.monitoring_service.monitor_instance_once(
                    instance_id=str(instance.id), runtime=self.strategy_runtime,
                    volume=volume, lookback_bars=lookback_bars,
                )
                decisions.append(StrategyDecision(
                    strategy_instance_id=instance.id, instrument=instance.instrument,
                    signal=float(result.signal), confidence=1.0,
                    reason=f"{result.strategy_name}:{result.strategy_version_id}",
                ))
            except Exception as exc:  # isolate one broken feed/instance
                errors[instance.name] = str(exc)
        volatilities, correlations = self._risk_inputs()
        return PortfolioRunResult(
            decisions=decisions,
            portfolio=self.allocator.allocate(
                decisions,
                annualized_volatility_by_symbol=volatilities,
                correlations=correlations,
            ),
            errors=errors,
        )

    def _risk_inputs(self) -> tuple[dict[str, float], dict[tuple[str, str], float]]:
        repository = getattr(self.monitoring_service, "market_data_repository", None)
        if repository is None:
            return {}, {}
        series: dict[str, dict[object, float]] = {}
        volatilities: dict[str, float] = {}
        for instance in self.strategy_repository.list_instances(enabled_only=True):
            try:
                bars = repository.fetch_latest_bars(
                    instance.instrument.symbol,
                    instance.primary_interval.value,
                    limit=120,
                    exchange=instance.instrument.exchange,
                )
                returns = {
                    current.datetime: current.close / previous.close - 1
                    for previous, current in zip(bars, bars[1:], strict=False)
                    if previous.close
                }
                if len(returns) < 2:
                    continue
                seconds = instance.primary_interval.seconds
                volatilities[instance.instrument.symbol] = stdev(returns.values()) * math.sqrt(31_536_000 / seconds)
                series[instance.instrument.symbol] = returns
            except Exception:
                continue
        correlations: dict[tuple[str, str], float] = {}
        symbols = sorted(series)
        for index, first in enumerate(symbols):
            for second in symbols[index + 1 :]:
                common = sorted(set(series[first]) & set(series[second]))
                if len(common) < 3:
                    continue
                left = [series[first][at] for at in common]
                right = [series[second][at] for at in common]
                try:
                    correlations[(first, second)] = correlation(left, right)
                except StatisticsError:
                    continue
        return volatilities, correlations
