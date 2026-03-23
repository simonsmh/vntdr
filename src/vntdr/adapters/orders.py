from __future__ import annotations

from typing import Any

import okx.Trade as Trade

from vntdr.models import OrderInstruction


class SimulatedOrderExecutor:
    def execute(self, instructions: list[OrderInstruction]) -> list[OrderInstruction]:
        return instructions


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
    ) -> None:
        self.margin_mode = margin_mode
        self.order_type = order_type
        self.trade_api = trade_api or Trade.TradeAPI(
            api_key=api_key,
            api_secret_key=secret_key,
            passphrase=passphrase,
            use_server_time=False,
            flag="1" if demo_trading else "0",
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
        return instructions

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
