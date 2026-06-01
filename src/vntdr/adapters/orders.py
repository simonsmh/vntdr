from __future__ import annotations

from typing import Any
import asyncio
from concurrent.futures import ThreadPoolExecutor

import okx.Trade as Trade
import okx.Account as Account
import okx.PublicData as PublicData

from vntdr.models import OrderInstruction

import logging
logger = logging.getLogger(__name__)


class SimulatedOrderExecutor:
    def execute(self, instructions: list[OrderInstruction]) -> list[OrderInstruction]:
        return instructions

    async def execute_async(self, instructions: list[OrderInstruction]) -> list[OrderInstruction]:
        return self.execute(instructions)

    def get_current_positions(self, inst_type: str = "SWAP") -> list[dict[str, Any]]:
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
        trade_api: Any | None = None,
        account_api: Any | None = None,
    ) -> None:
        self.margin_mode = margin_mode
        self.order_type = order_type
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
        for instruction in instructions:
            side, pos_side, reduce_only = self._translate_instruction(instruction.action)
            response = self.trade_api.place_order(
                instId=instruction.symbol,
                tdMode=self.margin_mode,
                side=side,
                posSide=pos_side,
                ordType=self.order_type,
                sz=self._format_volume(instruction.volume),
                reduceOnly=reduce_only,
            )
            if response.get("code") != "0":
                raise RuntimeError(f"OKX order rejected for {instruction.symbol}: {response}")
            logger.info(f"Placed order {instruction.action} for {instruction.symbol} size={instruction.volume}: {response}")
        return instructions

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
