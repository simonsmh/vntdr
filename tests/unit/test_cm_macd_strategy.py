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
