from __future__ import annotations

from vntdr.models import BarRecord
from vntdr.strategies.cm_macd_ult_mtf import Strategy


def test_cm_macd_strategy_emits_long_and_short_signals(
    sample_xau_bar_payloads: list[dict[str, object]],
) -> None:
    bars = [BarRecord.model_validate(payload) for payload in sample_xau_bar_payloads]
    parameters = {
        "fast_length": 3,
        "slow_length": 6,
        "signal_length": 3,
        "trend_window": 3,
    }

    signals = [Strategy.signal_for_index(bars, index, parameters) for index in range(len(bars))]

    assert 1 in signals
    assert -1 in signals


def test_cm_macd_strategy_recomputes_when_cached_fingerprint_differs(
    sample_xau_bar_payloads: list[dict[str, object]],
) -> None:
    bars = [BarRecord.model_validate(payload) for payload in sample_xau_bar_payloads]
    parameters = {
        "fast_length": 3,
        "slow_length": 6,
        "signal_length": 3,
        "trend_window": 3,
    }
    defaults = {**parameters}
    cache_key = (id(bars), tuple(sorted(defaults.items())))
    Strategy._cache[cache_key] = (("stale",), [1])

    signal = Strategy.signal_for_index(bars, len(bars) - 1, parameters)

    assert signal in {-1, 0, 1}
    assert len(Strategy._cache[cache_key][1]) == len(bars)
