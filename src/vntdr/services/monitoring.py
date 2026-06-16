from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Protocol

from vntdr.cleaning import INTERVAL_TO_DELTA
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
        processed_bar_key = f"processed_bar_ts:{symbol}:{interval}:{strategy_name}"

        previous_signal = self.signal_store.get(cache_key)
        last_processed_bar_ts = self.signal_store.get(processed_bar_key)

        bars = self.market_data_repository.fetch_latest_bars(symbol, interval, limit=lookback_bars)
        if not bars:
            raise ValueError("No bars available for monitoring.")

        completed_bars = self._completed_bars(bars, interval)

        if not completed_bars:
            raise ValueError("No completed bars available for monitoring.")
        completed_bar = completed_bars[-1]
        completed_bar_ts = int(completed_bar.datetime.timestamp())

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

        if last_processed_bar_ts == completed_bar_ts:
            stable_signal = 0 if previous_signal is None else previous_signal
            self._save_live_status(
                strategy_name=strategy_name,
                symbol=symbol,
                interval=interval,
                signal=stable_signal,
                previous_signal=previous_signal,
                best_parameters={},
                actions=[],
                notification_sent=False,
                execution_error=None,
                completed_bar_time=completed_bar.datetime.isoformat(),
                skipped_reason="already_processed_closed_bar",
            )
            return MonitorResult(
                symbol=symbol,
                interval=interval,
                strategy_name=strategy_name,
                signal=stable_signal,
                previous_signal=previous_signal,
                best_parameters={},
                actions=[],
                notification_sent=False,
                error=None,
            )

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

        confirmed_signal = self.research_service.latest_signal(
            strategy_name=strategy_name,
            bars=completed_bars,
            parameters=best_parameters,
        )

        self.risk_manager.validate_symbol(symbol)

        effective_prev = 0 if previous_signal is None else previous_signal
        is_bootstrap = previous_signal is None
        signal_changed = effective_prev != confirmed_signal
        if is_bootstrap:
            logger.info(
                "Bootstrapping signal state for %s %s %s at closed bar %s to %s; "
                "notification and orders are suppressed for the first observed state.",
                strategy_name,
                symbol,
                interval,
                completed_bar.datetime.isoformat(),
                confirmed_signal,
            )

        instructions = []
        if not is_bootstrap and signal_changed:
            instructions = self.risk_manager.filter_instructions(
                self._build_instructions(symbol, previous_signal, confirmed_signal, volume),
                previous_signal=previous_signal,
                next_signal=confirmed_signal,
            )

        notification_sent = False
        if not is_bootstrap and signal_changed:
            message = self._build_message(
                symbol=symbol,
                interval=interval,
                strategy_name=strategy_name,
                signal=confirmed_signal,
                previous_signal=previous_signal,
                parameters=best_parameters,
                actions=[instruction.action for instruction in instructions],
                close_price=completed_bar.close,
                signal_time=completed_bar.datetime,
            )
            try:
                self.notifier.notify(message)
                notification_sent = True
            except Exception as e:
                logger.error(f"Failed to send confirmation notification: {e}")

        execution_error = None
        if instructions:
            try:
                self.order_executor.execute(instructions)
            except Exception as e:
                execution_error = str(e)
                logger.error(f"Order execution failed: {e}")

        self.signal_store.set(cache_key, confirmed_signal)
        self.signal_store.set(processed_bar_key, completed_bar_ts)

        self._save_live_status(
            strategy_name=strategy_name,
            symbol=symbol,
            interval=interval,
            signal=confirmed_signal,
            previous_signal=previous_signal,
            best_parameters=best_parameters,
            actions=[instruction.action for instruction in instructions],
            notification_sent=notification_sent,
            execution_error=execution_error,
            completed_bar_time=completed_bar.datetime.isoformat(),
            skipped_reason=None,
        )

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

    def _completed_bars(self, bars: list[Any], interval: str) -> list[Any]:
        interval_lower = interval.lower()
        delta = INTERVAL_TO_DELTA.get(interval_lower)
        if delta is None and bars:
            delta = INTERVAL_TO_DELTA.get(str(bars[-1].interval).lower())
        if delta is None:
            raise ValueError(f"Unsupported interval for monitoring: {interval}")

        now_utc = datetime.now(timezone.utc)
        completed = []
        for bar in bars:
            bar_dt = bar.datetime
            if bar_dt.tzinfo is None:
                bar_dt = bar_dt.replace(tzinfo=timezone.utc)
            if now_utc >= bar_dt + delta:
                completed.append(bar)
        return completed

    def _save_live_status(
        self,
        *,
        strategy_name: str,
        symbol: str,
        interval: str,
        signal: int,
        previous_signal: int | None,
        best_parameters: dict[str, Any],
        actions: list[str],
        notification_sent: bool,
        execution_error: str | None,
        completed_bar_time: str,
        skipped_reason: str | None,
    ) -> None:
        if hasattr(self.signal_store, "client"):
            try:
                import json
                now = datetime.now(timezone.utc)
                status_entry = {
                    "time": now.strftime("%Y-%m-%d %H:%M:%S UTC"),
                    "strategy_name": strategy_name,
                    "symbol": symbol,
                    "interval": interval,
                    "signal": signal,
                    "previous_signal": previous_signal,
                    "best_parameters": best_parameters,
                    "actions": actions,
                    "notification_sent": notification_sent,
                    "error": execution_error,
                    "completed_bar_time": completed_bar_time,
                    "skipped_reason": skipped_reason,
                    "heartbeat": now.timestamp()
                }
                serialized = json.dumps(status_entry, ensure_ascii=False)
                self.signal_store.client.set("vntdr:live_status", serialized)
                self.signal_store.client.hset(
                    "vntdr:live_statuses",
                    f"{symbol}:{interval}:{strategy_name}",
                    serialized,
                )
                self.signal_store.client.lpush("vntdr:live_logs", serialized)
                self.signal_store.client.ltrim("vntdr:live_logs", 0, 99)
            except Exception as e:
                logger.warning(f"Failed to save live status/log to Redis: {e}")

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
        close_price: float,
        signal_time: datetime,
    ) -> str:
        from html import escape

        signal_map = {
            1: "LONG",
            -1: "SHORT",
            0: "空仓",
            None: "❓ 未知",
        }
        signal_icon_map = {1: "🔵", -1: "🔴", 0: "⚪", None: "❓"}
        action_map = {
            "buy_long": "开多",
            "sell_long": "平多",
            "sell_short": "开空",
            "buy_short": "平空",
        }

        drawdown = self.risk_manager.get_current_drawdown()
        drawdown_str = f"{drawdown:.2%}" if drawdown is not None else "N/A"

        esc_strategy = escape(strategy_name)
        esc_symbol = escape(symbol)
        esc_interval = escape(interval)
        icon = signal_icon_map.get(signal, "⚪")
        signal_text = signal_map.get(signal, str(signal))
        previous_text = signal_map.get(previous_signal, str(previous_signal))
        price_text = self._format_price(close_price)
        action_text = " + ".join(action_map.get(a, a) for a in actions) if actions else "无操作"
        params_text = self._format_parameters(parameters)

        if signal_time.tzinfo is None:
            signal_time = signal_time.replace(tzinfo=timezone.utc)
        signal_time_text = signal_time.astimezone(timezone.utc).strftime("%m-%d %H:%M UTC")

        return (
            f"{icon} <b>{esc_symbol} {esc_interval} {signal_text} @ {price_text}</b>\n"
            f"动作: <b>{escape(action_text)}</b>\n"
            f"信号: {escape(previous_text)} → {escape(signal_text)} | 收盘: {signal_time_text}\n"
            f"参数: <code>{escape(params_text)}</code>\n"
            f"策略: <code>{esc_strategy}</code> | 回撤: {drawdown_str}"
        )

    def _format_price(self, price: float) -> str:
        text = f"{price:.6f}".rstrip("0").rstrip(".")
        return text if text else "0"

    def _format_parameters(self, parameters: dict[str, Any]) -> str:
        aliases = {
            "fast_length": "fast",
            "slow_length": "slow",
            "signal_length": "sig",
            "trend_window": "tw",
        }
        return " ".join(
            f"{aliases.get(str(key), str(key))}={value}"
            for key, value in parameters.items()
        )

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
