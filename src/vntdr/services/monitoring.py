from __future__ import annotations

from typing import Any, Protocol
import asyncio
from concurrent.futures import ThreadPoolExecutor

import logging
from vntdr.models import MonitorResult, OrderInstruction
from vntdr.services.research import ResearchService
from vntdr.services.risk import RiskManager
from vntdr.storage.repositories import MarketDataRepository

logger = logging.getLogger(__name__)


class Notifier(Protocol):
    def notify(self, message: str) -> None: ...


class OrderExecutor(Protocol):
    def execute(self, instructions: list[OrderInstruction]) -> list[OrderInstruction]: ...
    def get_current_positions(self, symbol: str | None = None) -> list[dict[str, Any]]: ...
    def get_account_equity(self) -> float: ...


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
        self._executor = ThreadPoolExecutor(max_workers=4)

    def reconcile_positions(self, symbol: str) -> int | None:
        """
        Reconcile current position from OKX API and infer current signal.
        Returns the inferred signal based on open positions:
          1 = long position open
         -1 = short position open
          0 = no position open
         None = reconciliation couldn't determine (no positions)
        """
        logger.info(f"Reconciling positions for {symbol} from OKX API")
        positions = self.order_executor.get_current_positions(symbol=symbol)

        if not positions:
            logger.info(f"No open positions found for {symbol}, signal will default to 0 (no position)")
            return 0

        # For our strategy, we only hold one position at a time
        # Check if it's long or short
        for pos in positions:
            pos_side = pos.get("posSide", "")
            pos_size = float(pos.get("pos", "0"))
            if pos_size > 0 and pos_side == "long":
                logger.info(f"Found existing long position for {symbol}, reconciling signal to 1")
                return 1
            elif pos_size > 0 and pos_side == "short":
                logger.info(f"Found existing short position for {symbol}, reconciling signal to -1")
                return -1

        logger.info(f"No valid open positions found for {symbol}, reconciling signal to 0")
        return 0

    async def reconcile_positions_async(self, symbol: str) -> int | None:
        return await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self.reconcile_positions,
            symbol
        )

    def update_account_info(self) -> None:
        """Update account equity in risk manager for drawdown tracking."""
        try:
            current_equity = self.order_executor.get_account_equity()
            self.risk_manager.update_equity(current_equity)
            logger.debug(f"Updated account equity: {current_equity}")
        except Exception as e:
            logger.warning(f"Failed to update account equity for drawdown tracking: {e}")

    async def update_account_info_async(self) -> None:
        await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self.update_account_info
        )

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
        cache_key = f"signal:{symbol}:{interval}:{strategy_name}"
        notify_cache_key = f"notify:{symbol}:{interval}:{strategy_name}"

        previous_signal = self.signal_store.get(cache_key)
        last_notified = self.signal_store.get(notify_cache_key)

        bars = self.market_data_repository.fetch_latest_bars(symbol, interval, limit=lookback_bars)
        if not bars:
            raise ValueError("No bars available for monitoring.")

        # Update current account equity for drawdown checks
        self.update_account_info()

        # Try to reconcile previous signal if not in cache
        if previous_signal is None:
            try:
                reconciled = self.reconcile_positions(symbol)
                if reconciled is not None:
                    previous_signal = reconciled
                    self.signal_store.set(cache_key, previous_signal)
            except Exception as e:
                logger.warning(f"Reconciliation failed (API issues): {e}. Falling back to monitoring-only mode.")
                # We don't raise error here, just let previous_signal stay None
                # or we can treat it as 0 to start detecting changes from now.

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
        
        # Build instructions (even if we might not execute them, we need them for the notification)
        instructions = self.risk_manager.filter_instructions(
            self._build_instructions(symbol, previous_signal, signal, volume),
            previous_signal=previous_signal,
            next_signal=signal,
        )
        
        # Determine if we should notify
        # If previous_signal is None, we treat it as 0 for change detection to avoid missing the first signal
        effective_prev = 0 if previous_signal is None else previous_signal
        should_notify = (signal != last_notified and effective_prev != signal)
        
        notification_sent = False
        if should_notify:
            message = self._build_message(
                symbol=symbol,
                interval=interval,
                strategy_name=strategy_name,
                signal=signal,
                previous_signal=previous_signal,
                parameters=best_parameters,
                actions=[instruction.action for instruction in instructions],
            )
            try:
                self.notifier.notify(message)
                self.signal_store.set(notify_cache_key, signal)
                notification_sent = True
            except Exception as e:
                logger.error(f"Failed to send notification: {e}")

        # Try to execute orders
        execution_error = None
        if instructions:
            try:
                self.order_executor.execute(instructions)
            except Exception as e:
                execution_error = str(e)
                logger.error(f"Order execution failed: {e}")
                # We DO NOT return early. We want to finish the monitoring cycle.

        # CRITICAL: Always update the current signal in cache to prevent notification loops,
        # even if the order execution failed. 
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
            error=execution_error
        )

    async def monitor_once_async(
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
        return await asyncio.get_event_loop().run_in_executor(
            self._executor,
            lambda: self.monitor_once(
                strategy_name=strategy_name,
                symbol=symbol,
                interval=interval,
                parameter_space=parameter_space,
                volume=volume,
                method=method,
                lookback_bars=lookback_bars
            )
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
        # If previous is None (cache empty after restart), don't generate any instructions
        # We already did reconciliation to get previous signal from OKX API, so it shouldn't be None
        # If reconciliation failed, we don't know the position so we shouldn't open any positions
        if previous_signal is None:
            logger.info("Previous signal is None (cache empty), skipping instruction generation until we know the starting position")
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
        signal_map = {
            1: "🔵 LONG (看涨)",
            -1: "🔴 SHORT (看跌)",
            0: "⚪ 空仓",
            None: "❓ 未知",
        }
        action_map = {
            "buy_long": "✅ 开多",
            "sell_long": "❌ 平多",
            "sell_short": "✅ 开空",
            "buy_short": "❌ 平空",
        }
        
        drawdown = self.risk_manager.get_current_drawdown()
        drawdown_str = f"{drawdown:.2%}" if drawdown is not None else "N/A"
        
        action_text = "\n".join([f"  • {action_map.get(a, a)}" for a in actions]) if actions else "  无操作"
        
        params_text = "\n".join([f"  • {k}: {v}" for k, v in parameters.items()])
        
        return f"""📊 **交易信号更新**
策略: `{strategy_name}`
交易对: `{symbol} {interval}`

🔄 信号变化:
  之前: {signal_map.get(previous_signal, f'{previous_signal}')}
  当前: {signal_map.get(signal, f'{signal}')}

📈 需要执行操作:
{action_text}

⚙️ 参数:
{params_text}

📉 当前最大回撤: {drawdown_str}
"""
