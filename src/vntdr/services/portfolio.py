"""Portfolio aggregation with explicit, deterministic risk budgets."""
from __future__ import annotations

from collections import defaultdict

from vntdr.models import PortfolioDecision, StrategyDecision


class PortfolioAllocator:
    def __init__(
        self,
        *,
        max_strategy_weight: float = 0.30,
        max_symbol_weight: float = 0.30,
        max_asset_class_weight: float = 0.50,
        max_gross_exposure: float = 0.60,
        target_annual_volatility: float = 0.10,
        max_correlation_cluster_weight: float = 0.40,
        correlation_threshold: float = 0.80,
    ) -> None:
        self.max_strategy_weight = max_strategy_weight
        self.max_symbol_weight = max_symbol_weight
        self.max_asset_class_weight = max_asset_class_weight
        self.max_gross_exposure = max_gross_exposure
        self.target_annual_volatility = target_annual_volatility
        self.max_correlation_cluster_weight = max_correlation_cluster_weight
        self.correlation_threshold = correlation_threshold

    def allocate(
        self,
        decisions: list[StrategyDecision],
        *,
        annualized_volatility_by_symbol: dict[str, float] | None = None,
        correlations: dict[tuple[str, str], float] | None = None,
    ) -> PortfolioDecision:
        # Strategy signals first receive their individual cap and confidence.
        contributions: list[tuple[StrategyDecision, float]] = [
            (decision, max(-self.max_strategy_weight, min(self.max_strategy_weight, decision.signal * decision.confidence * self.max_strategy_weight)))
            for decision in decisions
        ]
        reasons: list[str] = []
        # Cap each asset class proportionally without changing relative views.
        by_class: dict[str, float] = defaultdict(float)
        for decision, weight in contributions:
            by_class[decision.instrument.asset_class] += abs(weight)
        capped: list[tuple[StrategyDecision, float]] = []
        for decision, weight in contributions:
            class_total = by_class[decision.instrument.asset_class]
            if class_total > self.max_asset_class_weight:
                weight *= self.max_asset_class_weight / class_total
                reason = f"asset_class_cap:{decision.instrument.asset_class}"
                if reason not in reasons:
                    reasons.append(reason)
            capped.append((decision, weight))
        # Net same-symbol strategies then cap each symbol.
        by_symbol: dict[str, float] = defaultdict(float)
        for decision, weight in capped:
            by_symbol[decision.instrument.symbol] += weight
        for symbol, weight in list(by_symbol.items()):
            if abs(weight) > self.max_symbol_weight:
                by_symbol[symbol] = self.max_symbol_weight if weight > 0 else -self.max_symbol_weight
                reasons.append(f"symbol_cap:{symbol}")
        annualized_volatility_by_symbol = annualized_volatility_by_symbol or {}
        for symbol, weight in list(by_symbol.items()):
            volatility = annualized_volatility_by_symbol.get(symbol)
            if volatility and volatility > self.target_annual_volatility:
                by_symbol[symbol] = weight * self.target_annual_volatility / volatility
                reasons.append(f"volatility_target:{symbol}")
        self._apply_correlation_cluster_caps(by_symbol, correlations or {}, reasons)
        gross = sum(abs(weight) for weight in by_symbol.values())
        if gross > self.max_gross_exposure:
            ratio = self.max_gross_exposure / gross
            by_symbol = {symbol: weight * ratio for symbol, weight in by_symbol.items()}
            gross = self.max_gross_exposure
            reasons.append("gross_exposure_cap")
        return PortfolioDecision(
            target_weights=dict(by_symbol), gross_exposure=round(gross, 8),
            net_exposure=round(sum(by_symbol.values()), 8), scaling_reasons=reasons,
        )

    def _apply_correlation_cluster_caps(
        self,
        weights: dict[str, float],
        correlations: dict[tuple[str, str], float],
        reasons: list[str],
    ) -> None:
        """Scale highly correlated symbol groups as a single risk cluster."""
        symbols = set(weights)
        groups: list[set[str]] = []
        for first, second in correlations:
            if first not in symbols or second not in symbols:
                continue
            if abs(correlations[(first, second)]) < self.correlation_threshold:
                continue
            overlapping = [group for group in groups if first in group or second in group]
            merged = {first, second}
            for group in overlapping:
                merged.update(group)
                groups.remove(group)
            groups.append(merged)
        for group in groups:
            exposure = sum(abs(weights[symbol]) for symbol in group)
            if exposure <= self.max_correlation_cluster_weight:
                continue
            scale = self.max_correlation_cluster_weight / exposure
            for symbol in group:
                weights[symbol] *= scale
            reasons.append(f"correlation_cluster_cap:{'|'.join(sorted(group))}")
