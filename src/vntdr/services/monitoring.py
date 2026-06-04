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
        parameter_space: dict[str, list[Any]] | None = None,
        volume: float,
        method: str = "ga",
        lookback_bars: int = 120,
    ) -> MonitorResult:
        cache_key = f"signal:{symbol}:{interval}:{strategy_name}"
        notify_cache_key = f"notify:{symbol}:{interval}:{strategy_name}"

        previous_signal = self.signal_store.get(cache_key)
        last_notified = self.signal_store.get(notify_cache_key)

        bars = self.market_data_repository.fetch_latest_bars(symbol, interval, limit=lookback_bars)
        if not bars:
            raise ValueError("No bars available for monitoring.")

        # Exclude the last bar if it is currently incomplete (still forming)
        # to prevent repaint / flashing signals from incomplete candles.
        is_last_incomplete = False
        incomplete_bar = None
        completed_bars = list(bars)
        full_bars = list(bars)
        
        if bars:
            last_bar = bars[-1]
            interval_lower = last_bar.interval.lower()
            from vntdr.cleaning import INTERVAL_TO_DELTA
            delta = INTERVAL_TO_DELTA.get(interval_lower)
            if delta:
                from datetime import datetime, timezone
                bar_dt = last_bar.datetime
                if bar_dt.tzinfo is None:
                    bar_dt = bar_dt.replace(tzinfo=timezone.utc)
                now_utc = datetime.now(timezone.utc)
                if now_utc < bar_dt + delta:
                    is_last_incomplete = True
                    incomplete_bar = last_bar
                    completed_bars = bars[:-1]

        if not completed_bars:
            raise ValueError("No completed bars available for monitoring.")

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

        # Parameter optimization on completed bars only for stability
        if parameter_space:
            best_parameters, _metrics, _ = self.research_service.optimize_parameters(
                strategy_name=strategy_name,
                bars=completed_bars,
                parameter_space=parameter_space,
                method=method,
                optimize_target=getattr(self.research_service.settings.research, "optimize_target", "sharpe"),
            )
        else:
            best_parameters = self.research_service.default_parameters(strategy_name)

        # 1. Confirmed Signal
        confirmed_signal = self.research_service.latest_signal(
            strategy_name=strategy_name,
            bars=completed_bars,
            parameters=best_parameters,
        )
        
        # 2. Potential Signal (Incomplete candle)
        potential_signal = confirmed_signal
        if is_last_incomplete:
            potential_signal = self.research_service.latest_signal(
                strategy_name=strategy_name,
                bars=full_bars,
                parameters=best_parameters,
            )

        self.risk_manager.validate_symbol(symbol)
        
        # Build instructions on confirmed_signal
        instructions = self.risk_manager.filter_instructions(
            self._build_instructions(symbol, previous_signal, confirmed_signal, volume),
            previous_signal=previous_signal,
            next_signal=confirmed_signal,
        )
        
        # Determine if we should notify confirmed signal changes
        effective_prev = 0 if previous_signal is None else previous_signal
        should_notify_confirmed = (confirmed_signal != last_notified and effective_prev != confirmed_signal)
        
        notification_sent = False
        if should_notify_confirmed:
            message = self._build_message(
                symbol=symbol,
                interval=interval,
                strategy_name=strategy_name,
                signal=confirmed_signal,
                previous_signal=previous_signal,
                parameters=best_parameters,
                actions=[instruction.action for instruction in instructions],
            )
            try:
                self.notifier.notify(message)
                self.signal_store.set(notify_cache_key, confirmed_signal)
                notification_sent = True
            except Exception as e:
                logger.error(f"Failed to send confirmation notification: {e}")

        # Send Potential Alert if there is an unconfirmed signal change
        if is_last_incomplete and potential_signal != confirmed_signal:
            potential_cache_key = f"potential_notify:{symbol}:{interval}:{strategy_name}:{incomplete_bar.datetime.isoformat()}"
            last_notified_potential = self.signal_store.get(potential_cache_key)
            
            if potential_signal != last_notified_potential:
                from datetime import datetime, timezone
                bar_dt = incomplete_bar.datetime
                if bar_dt.tzinfo is None:
                    bar_dt = bar_dt.replace(tzinfo=timezone.utc)
                now_utc = datetime.now(timezone.utc)
                remaining_sec = max(0, int(((bar_dt + delta) - now_utc).total_seconds()))
                h = remaining_sec // 3600
                m = (remaining_sec % 3600) // 60
                s = remaining_sec % 60
                remaining_str = f"{h}小时{m}分" if h > 0 else f"{m}分{s}秒"
                
                alert_message = self._build_potential_alert_message(
                    symbol=symbol,
                    interval=interval,
                    strategy_name=strategy_name,
                    confirmed_signal=confirmed_signal,
                    potential_signal=potential_signal,
                    remaining_str=remaining_str,
                )
                try:
                    self.notifier.notify(alert_message)
                    self.signal_store.set(potential_cache_key, potential_signal)
                    if hasattr(self.signal_store, "client"):
                        try:
                            self.signal_store.client.expire(potential_cache_key, 172800)
                        except Exception:
                            pass
                except Exception as e:
                    logger.error(f"Failed to send potential alert: {e}")

        # Try to execute orders on confirmed_signal
        execution_error = None
        if instructions:
            try:
                self.order_executor.execute(instructions)
            except Exception as e:
                execution_error = str(e)
                logger.error(f"Order execution failed: {e}")

        # CRITICAL: Always update the current confirmed signal in cache to prevent confirmation loops
        self.signal_store.set(cache_key, confirmed_signal)

        # Update dynamic monitoring status & log history in Redis for Gradio Dashboard
        if hasattr(self.signal_store, "client"):
            try:
                import json
                from datetime import datetime, timezone
                status_entry = {
                    "time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                    "strategy_name": strategy_name,
                    "symbol": symbol,
                    "interval": interval,
                    "signal": confirmed_signal,
                    "previous_signal": previous_signal,
                    "best_parameters": best_parameters,
                    "actions": [instruction.action for instruction in instructions],
                    "notification_sent": notification_sent,
                    "error": execution_error,
                    "heartbeat": datetime.now(timezone.utc).timestamp()
                }
                serialized = json.dumps(status_entry, ensure_ascii=False)
                self.signal_store.client.set("vntdr:live_status", serialized)
                self.signal_store.client.hset("vntdr:live_statuses", f"{symbol}:{interval}:{strategy_name}", serialized)
                self.signal_store.client.lpush("vntdr:live_logs", serialized)
                self.signal_store.client.ltrim("vntdr:live_logs", 0, 99)
            except Exception as e:
                logger.warning(f"Failed to save live status/log to Redis: {e}")
        
        return MonitorResult(
            symbol=symbol,
            interval=interval,
            strategy_name=strategy_name,
            signal=confirmed_signal,
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
        parameter_space: dict[str, list[Any]] | None = None,
        volume: float,
        method: str = "ga",
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
        from html import escape
        
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
        
        esc_strategy = escape(strategy_name)
        esc_symbol = escape(symbol)
        esc_interval = escape(interval)
        
        action_text = "\n".join([f"  • {action_map.get(a, a)}" for a in actions]) if actions else "  无操作"
        
        params_text = "\n".join([f"  • {escape(str(k))}: {escape(str(v))}" for k, v in parameters.items()])
        
        return f"""📊 <b>交易信号更新</b>
策略: <code>{esc_strategy}</code>
交易对: <code>{esc_symbol} {esc_interval}</code>

🔄 信号变化:
  之前: {signal_map.get(previous_signal, f'{previous_signal}')}
  当前: {signal_map.get(signal, f'{signal}')}

📈 需要执行操作:
{action_text}

⚙️ 参数:
{params_text}

📉 当前最大回撤: {drawdown_str}
"""

    def _build_potential_alert_message(
        self,
        *,
        symbol: str,
        interval: str,
        strategy_name: str,
        confirmed_signal: int,
        potential_signal: int,
        remaining_str: str,
    ) -> str:
        from html import escape
        
        signal_map = {
            1: "🔵 LONG (看涨)",
            -1: "🔴 SHORT (看跌)",
            0: "⚪ 空仓",
            None: "❓ 未知",
        }
        
        esc_strategy = escape(strategy_name)
        esc_symbol = escape(symbol)
        esc_interval = escape(interval)
        
        return f"""🔔 <b>盘中信号预警 (未收盘/仅供参考)</b>
策略: <code>{esc_strategy}</code>
交易对: <code>{esc_symbol} {esc_interval}</code>

🔄 预警变化:
  当前持仓状态: {signal_map.get(confirmed_signal, f'{confirmed_signal}')}
  盘中潜在新信号: {signal_map.get(potential_signal, f'{potential_signal}')}

⏳ 距离收盘还有: <b>{remaining_str}</b>
⚠️ <i>注意：盘中价格波动频繁，该信号尚未确认，系统暂未执行任何下单操作。实际下单将在收盘时以最终收盘价为准。</i>
"""
