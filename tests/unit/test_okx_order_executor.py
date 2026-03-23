from __future__ import annotations

import pytest

from vntdr.adapters.orders import OkxOrderExecutor
from vntdr.models import OrderInstruction


class FakeTradeApi:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    def place_order(self, **kwargs):
        self.calls.append(kwargs)
        return {"code": "0", "data": [{"ordId": "1"}]}


def test_okx_order_executor_translates_long_and_short_actions() -> None:
    trade_api = FakeTradeApi()
    executor = OkxOrderExecutor(
        api_key="key",
        secret_key="secret",
        passphrase="pass",
        demo_trading=True,
        trade_api=trade_api,
    )

    executor.execute(
        [
            OrderInstruction(symbol="XAUUSDT", action="buy_long", volume=1.0, reason="open long"),
            OrderInstruction(symbol="XAUUSDT", action="sell_long", volume=1.0, reason="close long"),
            OrderInstruction(symbol="XAUUSDT", action="sell_short", volume=2.0, reason="open short"),
            OrderInstruction(symbol="XAUUSDT", action="buy_short", volume=2.0, reason="close short"),
        ]
    )

    assert trade_api.calls == [
        {
            "instId": "XAUUSDT",
            "tdMode": "cross",
            "side": "buy",
            "posSide": "long",
            "ordType": "market",
            "sz": "1",
            "reduceOnly": "false",
        },
        {
            "instId": "XAUUSDT",
            "tdMode": "cross",
            "side": "sell",
            "posSide": "long",
            "ordType": "market",
            "sz": "1",
            "reduceOnly": "true",
        },
        {
            "instId": "XAUUSDT",
            "tdMode": "cross",
            "side": "sell",
            "posSide": "short",
            "ordType": "market",
            "sz": "2",
            "reduceOnly": "false",
        },
        {
            "instId": "XAUUSDT",
            "tdMode": "cross",
            "side": "buy",
            "posSide": "short",
            "ordType": "market",
            "sz": "2",
            "reduceOnly": "true",
        },
    ]


def test_okx_order_executor_raises_when_exchange_rejects_order() -> None:
    class RejectingTradeApi:
        def place_order(self, **kwargs):
            return {"code": "51000", "msg": "rejected", "data": []}

    executor = OkxOrderExecutor(
        api_key="key",
        secret_key="secret",
        passphrase="pass",
        demo_trading=True,
        trade_api=RejectingTradeApi(),
    )

    with pytest.raises(RuntimeError):
        executor.execute([OrderInstruction(symbol="XAUUSDT", action="buy_long", volume=1.0, reason="test")])
