from __future__ import annotations

import pytest

from vntdr.config import RiskSettings
from vntdr.models import OrderInstruction
from vntdr.services.risk import RiskManager


def test_risk_manager_rejects_symbol_outside_allowlist() -> None:
    manager = RiskManager(RiskSettings(allowed_symbols=["XAUUSDT"], max_order_size=1.0))

    with pytest.raises(ValueError):
        manager.validate_symbol("BTC-USDT-SWAP")


def test_risk_manager_rejects_oversized_order() -> None:
    manager = RiskManager(RiskSettings(allowed_symbols=["XAUUSDT"], max_order_size=1.0))

    with pytest.raises(ValueError):
        manager.filter_instructions(
            [OrderInstruction(symbol="XAUUSDT", action="buy_long", volume=2.0, reason="test")],
            previous_signal=0,
            next_signal=1,
        )


def test_risk_manager_can_block_opening_trades_but_allow_closing() -> None:
    manager = RiskManager(
        RiskSettings(
            allowed_symbols=["XAUUSDT"],
            max_order_size=1.0,
            allow_opening_trades=False,
        )
    )

    instructions = manager.filter_instructions(
        [
            OrderInstruction(symbol="XAUUSDT", action="sell_long", volume=1.0, reason="close long"),
            OrderInstruction(symbol="XAUUSDT", action="sell_short", volume=1.0, reason="open short"),
        ],
        previous_signal=1,
        next_signal=-1,
    )

    assert [instruction.action for instruction in instructions] == ["sell_long"]
