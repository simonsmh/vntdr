from __future__ import annotations

from vntdr.services.position_sizing import AtrRiskSizer


def test_atr_risk_sizer_uses_stop_distance_and_notional_cap() -> None:
    sized = AtrRiskSizer(risk_fraction=0.01, stop_atr_multiple=2, max_notional_fraction=0.30).size(
        equity=10_000, price=100, atr=2
    )
    assert sized.risk_budget == 100
    assert sized.stop_distance == 4
    assert sized.units == 25
    assert sized.notional == 2500
    assert sized.capped is False

    capped = AtrRiskSizer().size(equity=10_000, price=100, atr=0.01)
    assert capped.units == 30
    assert capped.capped is True
