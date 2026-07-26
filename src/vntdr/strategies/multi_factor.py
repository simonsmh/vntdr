"""Explainable OHLCV-only multi-factor baseline strategy.

It deliberately uses independent roles (trend regime, breakout momentum, and
volatility risk) rather than voting among correlated oscillators.  External
factors can be added through the same score inputs in a future plugin.
"""
from __future__ import annotations

from datetime import timedelta
from typing import Any

from vntdr.models import BarRecord, Interval
from vntdr.services.data_context import MarketDataContext
from vntdr.strategies.base import ReviewedStrategyBase

DEFAULT_PARAMETERS = {
    "trend_window": 50,
    "breakout_window": 20,
    "regime_window": 20,
    "min_efficiency": 0.15,
    "atr_window": 14,
    "max_atr_ratio": 0.04,
    "entry_threshold": 0.6,
    "exit_threshold": 0.2,
    "trend_weight": 0.5,
    "momentum_weight": 0.5,
    "daily_trend_weight": 0.0,
    "funding_weight": 0.0,
    "open_interest_weight": 0.0,
    "funding_rate_scale": 0.001,
    "open_interest_change_scale": 0.05,
    "min_holding_bars": 3,
    "cooldown_bars": 2,
    "enable_volatility": True,
    "enable_atr_sizing": True,
    "risk_fraction": 0.01,
    "stop_atr_multiple": 2.0,
    "max_notional_fraction": 0.30,
}

DEFAULT_PARAMETER_SPACE = {
    # This is deliberately a small, preregistered walk-forward space (64
    # combinations). Factor weights, derivative scales and execution controls
    # are versioned configuration, not knobs to refit on every fold.
    "trend_window": [30, 50],
    "breakout_window": [15, 20],
    "min_efficiency": [0.10, 0.20],
    "max_atr_ratio": [0.03, 0.04],
    "entry_threshold": [0.55, 0.65],
    "exit_threshold": [0.15, 0.25],
}


def _atr(bars: list[BarRecord], index: int, window: int) -> float:
    if index < 1:
        return 0.0
    start = max(1, index - window + 1)
    ranges = [max(bars[i].high - bars[i].low, abs(bars[i].high - bars[i - 1].close), abs(bars[i].low - bars[i - 1].close)) for i in range(start, index + 1)]
    return sum(ranges) / len(ranges)


def _ema(values: list[float], window: int) -> float:
    alpha = 2.0 / (window + 1)
    value = values[0]
    for current in values[1:]:
        value = alpha * current + (1 - alpha) * value
    return value


class Strategy(ReviewedStrategyBase):
    @classmethod
    def score_for_index(cls, bars: list[BarRecord], index: int, parameters: dict[str, Any]) -> tuple[float, dict[str, float]]:
        p = {**DEFAULT_PARAMETERS, **parameters}
        trend_window, breakout_window, regime_window, atr_window = (
            int(p[name])
            for name in ("trend_window", "breakout_window", "regime_window", "atr_window")
        )
        warmup = max(trend_window, breakout_window, regime_window, atr_window)
        if index < warmup or not bars[index].close:
            return 0.0, {
                "trend": 0.0, "momentum": 0.0,
                "regime": 0.0, "volatility": 0.0,
            }
        close = bars[index].close
        atr = _atr(bars, index, atr_window)
        ema = _ema(
            [bar.close for bar in bars[index - trend_window + 1 : index + 1]],
            trend_window,
        )
        # Continuous distance-to-trend score saturates at two ATRs. Unlike a
        # same-window breakout flag this remains informative inside the range.
        trend = max(-1.0, min(1.0, (close - ema) / (2 * atr))) if atr else 0.0
        prior = bars[index - breakout_window:index]
        range_high, range_low = max(bar.high for bar in prior), min(bar.low for bar in prior)
        momentum = (
            2 * (close - range_low) / (range_high - range_low) - 1
            if range_high > range_low
            else 0.0
        )
        momentum = max(-1.0, min(1.0, momentum))
        changes = [
            abs(bars[i].close - bars[i - 1].close)
            for i in range(index - regime_window + 1, index + 1)
        ]
        path = sum(changes)
        efficiency = (
            abs(close - bars[index - regime_window].close) / path
            if path
            else 0.0
        )
        regime = 1.0 if efficiency >= float(p["min_efficiency"]) else 0.0
        atr_ratio = atr / close
        volatility = 1.0 if (not p["enable_volatility"] or atr_ratio <= float(p["max_atr_ratio"])) else 0.0
        # Trend and momentum determine direction; regime and volatility gate it.
        trend_weight, momentum_weight = float(p["trend_weight"]), float(p["momentum_weight"])
        total_weight = trend_weight + momentum_weight
        score = 0.0 if total_weight == 0 else (trend_weight * trend + momentum_weight * momentum) / total_weight
        return regime * volatility * score, {
            "trend": trend,
            "momentum": momentum,
            "regime": regime,
            "volatility": volatility,
        }

    @classmethod
    def signal_for_index(cls, bars: list[BarRecord], index: int, parameters: dict[str, Any]) -> int:
        return cls.target_position_for_index(bars, index, parameters, 0)

    @classmethod
    def target_position_for_index(
        cls,
        bars: list[BarRecord],
        index: int,
        parameters: dict[str, Any],
        current_position: int,
    ) -> int:
        score, _ = cls.score_for_index(bars, index, parameters)
        p = {**DEFAULT_PARAMETERS, **parameters}
        entry = float(p["entry_threshold"])
        exit_ = float(p["exit_threshold"])
        if score >= entry:
            return 1
        if score <= -entry:
            return -1
        if current_position > 0 and score > exit_:
            return 1
        if current_position < 0 and score < -exit_:
            return -1
        return 0

    @classmethod
    def target_position_for_context(
        cls,
        bars: list[BarRecord],
        index: int,
        parameters: dict[str, Any],
        current_position: int,
        data_context: MarketDataContext,
    ) -> int:
        """Combine only factors that were available at the decision timestamp."""
        p = {**DEFAULT_PARAMETERS, **parameters}
        daily_weight = float(p["daily_trend_weight"])
        funding_weight = float(p["funding_weight"])
        open_interest_weight = float(p["open_interest_weight"])
        if daily_weight == funding_weight == open_interest_weight == 0:
            return cls.target_position_for_index(bars, index, p, current_position)
        primary_score, _ = cls.score_for_index(bars, index, p)
        decision_at = bars[index].datetime + timedelta(
            seconds=Interval(value=bars[index].interval).seconds
        )
        components = [(primary_score, 1.0)]
        if daily_weight:
            daily = data_context.closed_bars("1d", decision_at)
            if len(daily) >= 20:
                daily_ema = _ema([bar.close for bar in daily[-20:]], 20)
                components.append((1.0 if daily[-1].close > daily_ema else -1.0, daily_weight))
        if funding_weight:
            funding = data_context.latest_factor("okx_funding_rate", decision_at)
            if funding is not None:
                scale = float(p["funding_rate_scale"])
                score = -funding.value / scale if scale else 0.0
                components.append((max(-1.0, min(1.0, score)), funding_weight))
        if open_interest_weight:
            observations = data_context.available_factors("okx_open_interest", decision_at)
            if len(observations) >= 2 and observations[-2].value:
                change = observations[-1].value / observations[-2].value - 1.0
                scale = float(p["open_interest_change_scale"])
                score = change / scale if scale else 0.0
                components.append((max(-1.0, min(1.0, score)), open_interest_weight))
        total_weight = sum(weight for _, weight in components)
        score = sum(value * weight for value, weight in components) / total_weight
        entry, exit_ = float(p["entry_threshold"]), float(p["exit_threshold"])
        if score >= entry:
            return 1
        if score <= -entry:
            return -1
        if current_position > 0 and score > exit_:
            return 1
        if current_position < 0 and score < -exit_:
            return -1
        return 0
