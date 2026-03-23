from __future__ import annotations

from vntdr.config import RiskSettings
from vntdr.models import OrderInstruction


class RiskManager:
    def __init__(self, settings: RiskSettings) -> None:
        self.settings = settings

    def validate_symbol(self, symbol: str) -> None:
        if symbol not in self.settings.allowed_symbols:
            raise ValueError(f"Symbol {symbol} is not in the allowed trading list.")

    def filter_instructions(
        self,
        instructions: list[OrderInstruction],
        *,
        previous_signal: int | None,
        next_signal: int,
    ) -> list[OrderInstruction]:
        filtered: list[OrderInstruction] = []
        for instruction in instructions:
            self.validate_symbol(instruction.symbol)
            if instruction.volume > self.settings.max_order_size:
                raise ValueError(
                    f"Order size {instruction.volume} exceeds max allowed size {self.settings.max_order_size}."
                )
            if not self.settings.allow_opening_trades and self._is_opening_action(
                action=instruction.action,
                previous_signal=previous_signal,
                next_signal=next_signal,
            ):
                continue
            filtered.append(instruction)
        return filtered

    def _is_opening_action(self, *, action: str, previous_signal: int | None, next_signal: int) -> bool:
        if action == "buy_long":
            return next_signal == 1
        if action == "sell_short":
            return next_signal == -1
        return False
