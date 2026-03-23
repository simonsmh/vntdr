from __future__ import annotations

from typing import Any, Protocol

from vntdr.models import MonitorResult, OrderInstruction
from vntdr.services.research import ResearchService
from vntdr.services.risk import RiskManager
from vntdr.storage.repositories import MarketDataRepository


class Notifier(Protocol):
    def notify(self, message: str) -> None: ...


class OrderExecutor(Protocol):
    def execute(self, instructions: list[OrderInstruction]) -> list[OrderInstruction]: ...


class SignalStore(Protocol):
    def get(self, key: str) -> int | None: ...

    def set(self, key: str, value: int) -> None: ...


class MonitoringService:
    def __init__(
        self,
        *,
        research_service: ResearchService,
        market_data_repository: MarketDataRepository,
        notifier: Notifier,
        order_executor: OrderExecutor,
        signal_store: SignalStore,
        risk_manager: RiskManager,
    ) -> None:
        self.research_service = research_service
        self.market_data_repository = market_data_repository
        self.notifier = notifier
        self.order_executor = order_executor
        self.signal_store = signal_store
        self.risk_manager = risk_manager

    def monitor_once(
        self,
        *,
        strategy_name: str,
        symbol: str,
        interval: str,
        parameter_space: dict[str, list[Any]],
        volume: float,
        method: str = "grid",
        lookback_bars: int = 120,
    ) -> MonitorResult:
        bars = self.market_data_repository.fetch_latest_bars(symbol, interval, limit=lookback_bars)
        if not bars:
            raise ValueError("No bars available for monitoring.")

        if parameter_space:
            best_parameters, _metrics, _ = self.research_service.optimize_parameters(
                strategy_name=strategy_name,
                bars=bars,
                parameter_space=parameter_space,
                method=method,
            )
        else:
            best_parameters = self.research_service.default_parameters(strategy_name)

        signal = self.research_service.latest_signal(
            strategy_name=strategy_name,
            bars=bars,
            parameters=best_parameters,
        )
        self.risk_manager.validate_symbol(symbol)
        cache_key = f"signal:{symbol}:{interval}:{strategy_name}"
        previous_signal = self.signal_store.get(cache_key)
        instructions = self.risk_manager.filter_instructions(
            self._build_instructions(symbol, previous_signal, signal, volume),
            previous_signal=previous_signal,
            next_signal=signal,
        )
        if previous_signal != signal:
            message = self._build_message(
                symbol=symbol,
                interval=interval,
                strategy_name=strategy_name,
                signal=signal,
                previous_signal=previous_signal,
                parameters=best_parameters,
                actions=[instruction.action for instruction in instructions],
            )
            self.notifier.notify(message)
            notification_sent = True
        else:
            notification_sent = False
        if instructions:
            self.order_executor.execute(instructions)
        self.signal_store.set(cache_key, signal)
        return MonitorResult(
            symbol=symbol,
            interval=interval,
            strategy_name=strategy_name,
            signal=signal,
            previous_signal=previous_signal,
            best_parameters=best_parameters,
            actions=[instruction.action for instruction in instructions],
            notification_sent=notification_sent,
        )

    def _build_instructions(
        self,
        symbol: str,
        previous_signal: int | None,
        signal: int,
        volume: float,
    ) -> list[OrderInstruction]:
        instructions: list[OrderInstruction] = []
        if previous_signal == signal:
            return instructions
        if previous_signal == 1 and signal <= 0:
            instructions.append(
                OrderInstruction(symbol=symbol, action="sell_long", volume=volume, reason="close long")
            )
        if previous_signal == -1 and signal >= 0:
            instructions.append(
                OrderInstruction(symbol=symbol, action="buy_short", volume=volume, reason="close short")
            )
        if signal == 1:
            instructions.append(
                OrderInstruction(symbol=symbol, action="buy_long", volume=volume, reason="open long")
            )
        elif signal == -1:
            instructions.append(
                OrderInstruction(symbol=symbol, action="sell_short", volume=volume, reason="open short")
            )
        return instructions

    def _build_message(
        self,
        *,
        symbol: str,
        interval: str,
        strategy_name: str,
        signal: int,
        previous_signal: int | None,
        parameters: dict[str, Any],
        actions: list[str],
    ) -> str:
        return (
            f"{strategy_name} signal update for {symbol} {interval}: "
            f"previous={previous_signal} current={signal} actions={actions} parameters={parameters}"
        )
