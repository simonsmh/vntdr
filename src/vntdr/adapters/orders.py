from __future__ import annotations

from typing import Any
import asyncio
from concurrent.futures import ThreadPoolExecutor

import okx.Trade as Trade
import okx.Account as Account
import okx.PublicData as PublicData
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from vntdr.models import OrderInstruction

import logging
logger = logging.getLogger(__name__)

# OKX sCode/code 值,代表瞬时错误,值得重试(系统繁忙/超时/限流)
TRANSIENT_ORDER_CODES = frozenset({"50013", "50026", "50004", "50011"})


class TransientOrderError(RuntimeError):
    """下单遇到 OKX 瞬时错误(如系统繁忙),可重试。"""


class PermanentOrderError(RuntimeError):
    """下单遇到 OKX 永久错误(如保证金不足/参数错),重试无意义。"""


class SimulatedOrderExecutor:
    def execute(self, instructions: list[OrderInstruction]) -> list[OrderInstruction]:
        return instructions

    async def execute_async(self, instructions: list[OrderInstruction]) -> list[OrderInstruction]:
        return self.execute(instructions)

    def get_current_positions(self, symbol: str | None = None) -> list[dict[str, Any]]:
        return []

    async def get_current_positions_async(self, symbol: str | None = None) -> list[dict[str, Any]]:
        return self.get_current_positions(symbol)

    def get_account_equity(self) -> float:
        return 0.0

    async def get_account_equity_async(self) -> float:
        return self.get_account_equity()


class OkxOrderExecutor:
    def __init__(
        self,
        *,
        api_key: str,
        secret_key: str,
        passphrase: str,
        demo_trading: bool,
        margin_mode: str = "cross",
        order_type: str = "market",
        order_retry_count: int = 3,
        order_retry_wait_seconds: float = 1.0,
        trade_api: Any | None = None,
        account_api: Any | None = None,
    ) -> None:
        self.margin_mode = margin_mode
        self.order_type = order_type
        self.order_retry_count = max(1, order_retry_count)
        self.order_retry_wait_seconds = max(0.0, order_retry_wait_seconds)
        flag = "1" if demo_trading else "0"
        self.trade_api = trade_api or Trade.TradeAPI(
            api_key=api_key,
            api_secret_key=secret_key,
            passphrase=passphrase,
            use_server_time=False,
            flag=flag,
        )
        self.account_api = account_api or Account.AccountAPI(
            api_key=api_key,
            api_secret_key=secret_key,
            passphrase=passphrase,
            use_server_time=False,
            flag=flag,
        )
        # Thread pool for running blocking I/O operations
        self._executor = ThreadPoolExecutor(max_workers=4)

    async def execute_async(self, instructions: list[OrderInstruction]) -> list[OrderInstruction]:
        return await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self.execute,
            instructions
        )

    def execute(self, instructions: list[OrderInstruction]) -> list[OrderInstruction]:
        # 平仓单(reduceOnly)即使最终失败也不中断整批,避免开仓腿成功后因平仓腿抛错而留下裸仓位。
        # 开仓单失败则立即抛出,阻止在不确定状态下继续。
        close_failures: list[str] = []
        for instruction in instructions:
            side, pos_side, reduce_only = self._translate_instruction(instruction.action)
            is_close = reduce_only == "true"
            try:
                self._place_one_with_retry(instruction, side, pos_side, reduce_only)
            except (TransientOrderError, PermanentOrderError) as exc:
                if is_close:
                    # 平仓失败最危险(留裸仓),记 critical 但继续执行剩余指令
                    logger.critical(
                        f"Close order FAILED after retries for {instruction.symbol} "
                        f"({instruction.action}); position may be left open: {exc}"
                    )
                    close_failures.append(f"{instruction.action}({instruction.symbol}): {exc}")
                    continue
                # 开仓失败:立即抛,不再执行后续指令
                raise
        if close_failures:
            raise RuntimeError(
                "Some close orders failed after retries (positions may be left open): "
                + "; ".join(close_failures)
            )
        return instructions

    def _place_one_with_retry(
        self,
        instruction: OrderInstruction,
        side: str,
        pos_side: str,
        reduce_only: str,
    ) -> None:
        """下单一笔,对瞬时错误(系统繁忙等)按指数退避重试;永久错误立即抛。"""
        retryer = Retrying(
            stop=stop_after_attempt(self.order_retry_count),
            wait=wait_exponential(multiplier=self.order_retry_wait_seconds, min=self.order_retry_wait_seconds),
            retry=retry_if_exception_type(TransientOrderError),
            reraise=True,
        )
        retryer(self._place_one, instruction, side, pos_side, reduce_only)

    def _place_one(
        self,
        instruction: OrderInstruction,
        side: str,
        pos_side: str,
        reduce_only: str,
    ) -> None:
        """提交单笔下单,根据返回码区分瞬时/永久错误。"""
        response = self.trade_api.place_order(
            instId=instruction.symbol,
            tdMode=self.margin_mode,
            side=side,
            posSide=pos_side,
            ordType=self.order_type,
            sz=self._format_volume(instruction.volume),
            reduceOnly=reduce_only,
        )
        code = response.get("code")
        if code == "0":
            logger.info(
                f"Placed order {instruction.action} for {instruction.symbol} "
                f"size={instruction.volume}: {response}"
            )
            return
        # 顶层 code 非 0;细化错误码可能在 data[0].sCode
        s_code = ""
        data = response.get("data") or []
        if data and isinstance(data[0], dict):
            s_code = data[0].get("sCode", "")
        if code in TRANSIENT_ORDER_CODES or s_code in TRANSIENT_ORDER_CODES:
            logger.warning(
                f"Transient OKX error for {instruction.symbol} ({instruction.action}), will retry: {response}"
            )
            raise TransientOrderError(f"OKX transient error for {instruction.symbol}: {response}")
        raise PermanentOrderError(f"OKX order rejected for {instruction.symbol}: {response}")

    async def get_current_positions_async(self, symbol: str | None = None) -> list[dict[str, Any]]:
        return await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self.get_current_positions,
            symbol
        )

    def get_current_positions(self, symbol: str | None = None) -> list[dict[str, Any]]:
        """Fetch current open positions from OKX. Optionally filter by symbol."""
        response = self.account_api.get_positions()
        if response.get("code") != "0":
            raise RuntimeError(f"Failed to fetch positions from OKX: {response}")
        positions = response.get("data", [])
        # Filter positions that have non-zero size
        open_positions = [
            pos for pos in positions 
            if float(pos.get("avgPx", "0")) > 0 and float(pos.get("pos", "0")) != 0
        ]
        if symbol:
            open_positions = [pos for pos in open_positions if pos.get("instId") == symbol]
        logger.info(f"Fetched {len(open_positions)} open positions from OKX")
        return open_positions

    async def get_account_equity_async(self) -> float:
        return await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self.get_account_equity
        )

    def get_account_equity(self) -> float:
        """Get total account equity in USDT."""
        response = self.account_api.get_account_balance()
        if response.get("code") != "0":
            raise RuntimeError(f"Failed to fetch account balance from OKX: {response}")
        data = response.get("data", [{}])[0]
        total_eq = float(data.get("totalEq", "0"))
        logger.debug(f"Current account equity: {total_eq} USDT")
        return total_eq

    def _translate_instruction(self, action: str) -> tuple[str, str, str]:
        mapping = {
            "buy_long": ("buy", "long", "false"),
            "sell_long": ("sell", "long", "true"),
            "sell_short": ("sell", "short", "false"),
            "buy_short": ("buy", "short", "true"),
        }
        try:
            return mapping[action]
        except KeyError as exc:
            raise ValueError(f"Unsupported order action: {action}") from exc

    def _format_volume(self, volume: float) -> str:
        return format(volume, "g")
