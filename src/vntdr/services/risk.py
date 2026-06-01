from __future__ import annotations

from vntdr.config import RiskSettings
from vntdr.models import OrderInstruction

import logging
logger = logging.getLogger(__name__)


class RiskManager:
    def __init__(self, settings: RiskSettings) -> None:
        self.settings = settings
        self._peak_equity: float | None = None
        self._current_equity: float | None = None

    def update_equity(self, current_equity: float) -> None:
        """Update current equity and track peak equity for drawdown calculation."""
        self._current_equity = current_equity
        if self._peak_equity is None or current_equity > self._peak_equity:
            self._peak_equity = current_equity

    def get_current_drawdown(self) -> float | None:
        """Calculate current drawdown from peak equity. Returns None if equity not set."""
        if self._current_equity is None or self._peak_equity is None:
            return None
        if self._peak_equity == 0:
            return 0.0
        return (self._peak_equity - self._current_equity) / self._peak_equity

    def check_max_drawdown(self) -> bool:
        """Return True if current drawdown exceeds max allowed drawdown."""
        drawdown = self.get_current_drawdown()
        if drawdown is None:
            # If we don't have equity data yet, allow trading
            return False
        exceeds = drawdown > self.settings.max_drawdown
        if exceeds:
            logger.warning(
                f"Max drawdown exceeded: current={drawdown:.2%}, limit={self.settings.max_drawdown:.2%}"
            )
        return exceeds

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
        if self.check_max_drawdown():
            # Reject all new opening trades when max drawdown is exceeded
            for instruction in instructions:
                self.validate_symbol(instruction.symbol)
                if instruction.volume > self.settings.max_order_size:
                    raise ValueError(
                        f"Order size {instruction.volume} exceeds max allowed size {self.settings.max_order_size}."
                    )
                # Still allow closing trades even when drawdown limit is hit
                if self._is_opening_action(
                    action=instruction.action,
                    previous_signal=previous_signal,
                    next_signal=next_signal,
                ):
                    logger.info(f"Rejecting opening trade {instruction.action} due to max drawdown exceeded")
                    continue
                filtered.append(instruction)
            return filtered

        # Normal filtering when drawdown is within limits
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
