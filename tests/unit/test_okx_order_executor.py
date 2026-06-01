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


class SequencedTradeApi:
    """按预设的返回码序列依次响应,记录调用次数。"""

    def __init__(self, responses: list[dict]) -> None:
        self.responses = responses
        self.calls: list[dict[str, str]] = []

    def place_order(self, **kwargs):
        self.calls.append(kwargs)
        index = min(len(self.calls) - 1, len(self.responses) - 1)
        return self.responses[index]


def _executor_with(trade_api, **kwargs) -> OkxOrderExecutor:
    return OkxOrderExecutor(
        api_key="key",
        secret_key="secret",
        passphrase="pass",
        demo_trading=True,
        order_retry_wait_seconds=0.0,  # 测试中不真正 sleep
        trade_api=trade_api,
        **kwargs,
    )


def test_transient_error_is_retried_until_success() -> None:
    # 前两次返回 50013(系统繁忙,瞬时),第三次成功
    trade_api = SequencedTradeApi(
        [
            {"code": "1", "data": [{"sCode": "50013", "sMsg": "Systems are busy"}]},
            {"code": "1", "data": [{"sCode": "50013", "sMsg": "Systems are busy"}]},
            {"code": "0", "data": [{"ordId": "1"}]},
        ]
    )
    executor = _executor_with(trade_api, order_retry_count=3)

    executor.execute([OrderInstruction(symbol="XAUUSDT", action="buy_long", volume=1.0, reason="open")])

    assert len(trade_api.calls) == 3  # 重试到第三次成功


def test_permanent_error_is_not_retried() -> None:
    # 51008 保证金不足是永久错误,应立即抛且只调用一次
    trade_api = SequencedTradeApi(
        [{"code": "1", "data": [{"sCode": "51008", "sMsg": "Insufficient margin"}]}]
    )
    executor = _executor_with(trade_api, order_retry_count=3)

    with pytest.raises(RuntimeError):
        executor.execute([OrderInstruction(symbol="XAUUSDT", action="buy_long", volume=1.0, reason="open")])

    assert len(trade_api.calls) == 1  # 永久错误不浪费重试


def test_open_failure_aborts_remaining_instructions() -> None:
    # 开仓失败应立即抛,阻止后续指令执行
    trade_api = SequencedTradeApi(
        [{"code": "1", "data": [{"sCode": "51008", "sMsg": "Insufficient margin"}]}]
    )
    executor = _executor_with(trade_api, order_retry_count=2)

    with pytest.raises(RuntimeError):
        executor.execute(
            [
                OrderInstruction(symbol="XAUUSDT", action="buy_long", volume=1.0, reason="open"),
                OrderInstruction(symbol="XAUUSDT", action="sell_short", volume=1.0, reason="open2"),
            ]
        )

    assert len(trade_api.calls) == 1  # 第一条开仓失败后不再执行第二条


def test_close_failure_does_not_abort_batch_but_raises_at_end() -> None:
    # 第一条平多持续 50013 失败(重试耗尽),第二条开空成功;
    # execute 应执行完两条,但末尾因平仓失败聚合抛错
    class RoutingTradeApi:
        def __init__(self) -> None:
            self.calls: list[dict[str, str]] = []

        def place_order(self, **kwargs):
            self.calls.append(kwargs)
            if kwargs.get("reduceOnly") == "true":
                return {"code": "1", "data": [{"sCode": "50013", "sMsg": "busy"}]}
            return {"code": "0", "data": [{"ordId": "ok"}]}

    trade_api = RoutingTradeApi()
    executor = _executor_with(trade_api, order_retry_count=2)

    with pytest.raises(RuntimeError, match="positions may be left open"):
        executor.execute(
            [
                OrderInstruction(symbol="XAUUSDT", action="sell_long", volume=1.0, reason="close long"),
                OrderInstruction(symbol="XAUUSDT", action="sell_short", volume=1.0, reason="open short"),
            ]
        )

    # 平多重试 2 次(都失败) + 开空 1 次成功 = 3 次调用;开空腿确实被执行了
    assert len(trade_api.calls) == 3
    assert any(c.get("posSide") == "short" and c.get("reduceOnly") == "false" for c in trade_api.calls)
