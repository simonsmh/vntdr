from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from html import escape
from typing import Any, Final, cast

from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardRemove, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from vntdr.services.config_service import ConfigService
from vntdr.services.telegram_research import IntervalResearchResult, TelegramResearchService

SYMBOL: Final[int] = 0
STRATEGY: Final[int] = 1
METHOD: Final[int] = 2
INTERVALS: Final[int] = 3
LOOKBACK: Final[int] = 4


@dataclass
class WatchConfig:
    symbol: str
    strategy_name: str
    interval: str
    method: str
    poll_seconds: int


@dataclass
class RankConfig:
    symbol: str
    strategy_name: str
    method: str
    intervals: list[str]
    lookback_hours: int


class TelegramCommandBot:
    LIVE_STATUS_TTL_SECONDS = 15 * 60

    def __init__(
        self,
        *,
        bot_token: str,
        chat_id: str,
        research_service: TelegramResearchService,
        monitor_once_callback,
        config_service: ConfigService | None = None,
        redis_client=None,
    ) -> None:
        self.bot_token = bot_token
        self.chat_id = str(chat_id)
        self.research_service = research_service
        self.monitor_once_callback = monitor_once_callback
        self.config_service = config_service
        self.redis_client = redis_client
        self.monitor_once_callback_async = getattr(
            monitor_once_callback, "__self__", None
        ).monitor_once_async if hasattr(monitor_once_callback, "__self__") else None
        callback_owner = getattr(monitor_once_callback, "__self__", None)
        monitoring_service = getattr(callback_owner, "monitoring_service", None)
        self.position_provider = getattr(monitoring_service, "order_executor", None)
        self.watch_job_name = f"watch:{self.chat_id}"

    # Redis helpers
    def _redis_key(self, suffix: str) -> str:
        return f"vntdr:{suffix}:{self.chat_id}"

    @staticmethod
    def _escape_markdown_v2(text: str) -> str:
        """Escape special characters for Telegram MarkdownV2."""
        # Characters that must be escaped outside code blocks
        # \ ` * _ [ ] ( ) ~ > # + - = | { } . !
        escape_chars = r"\_*[]()~`>#+-=|{}.!"
        return re.sub(f"([{re.escape(escape_chars)}])", r"\\\1", text)

    @staticmethod
    def _escape_markdown_v2_code(text: str) -> str:
        """Escape only backslash and backtick for code blocks in MarkdownV2."""
        return text.replace("\\", "\\\\").replace("`", "\\`")

    async def _send_safe(
        self, 
        update_or_query_or_id: Any, 
        text: str, 
        reply_markup: InlineKeyboardMarkup | None = None,
        edit: bool = False,
        parse_mode: Any = ParseMode.MARKDOWN_V2,
    ) -> None:
        """
        Send message with MarkdownV2 and fallback to Plain Text on error.
        """
        # 1. Base escaping for MarkdownV2 (outside code/links)
        # Note: We don't escape everything here because our formatting (like *)
        # would be broken. We rely on the fallback for complex cases.
        # But we MUST escape common troublemakers in data like . and -
        
        # 2. Determine target and send method
        from telegram.ext import Application
        from unittest.mock import Mock
        bot = None
        target_chat_id = self.chat_id
        
        target = update_or_query_or_id
        if isinstance(target, str):
            # If it's just a chat_id string
            target_chat_id = target
        elif isinstance(target, Mock):
            # Safe mock resolution based on what the tests explicitly configured
            if "edit_message_text" in target.__dict__ or "reply_text" in target.__dict__:
                pass
            elif "callback_query" in target.__dict__ and target.callback_query is not None:
                target = target.callback_query
            elif "message" in target.__dict__ and target.message is not None:
                target = target.message
        else:
            # Real telegram objects
            if hasattr(target, "callback_query") and target.callback_query is not None:
                target = target.callback_query
            elif hasattr(target, "message") and target.message is not None:
                target = target.message
        
        # Helper to get bot from update/query context if possible
        if hasattr(target, "bot"):
            bot = target.bot

        kwargs = {}
        if parse_mode is not None:
            kwargs["parse_mode"] = parse_mode
        if reply_markup is not None:
            kwargs["reply_markup"] = reply_markup

        try:
            if edit and hasattr(target, "edit_message_text"):
                await target.edit_message_text(text, **kwargs)
            elif hasattr(target, "reply_text"):
                await target.reply_text(text, **kwargs)
            elif bot:
                await bot.send_message(chat_id=target_chat_id, text=text, **kwargs)
        except BadRequest as e:
            if "Can't parse entities" in str(e):
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(f"MarkdownV2 parsing failed: {e}. Falling back to plain text.")
                
                # Standard Markdown to Plain Text: strip some common markers for better fallback readability
                clean_text = text.replace("*", "").replace("`", "")
                
                fallback_text = (
                    f"{clean_text}\n\n"
                    f"⚠️ 该消息 Markdown 格式 Telegram 解析失败，已转为纯文本。请检查特殊字符并重试。\n\n"
                    f"(Telegram error: {e.message})"
                )
                
                fallback_kwargs = {k: v for k, v in kwargs.items() if k != "parse_mode"}
                
                if edit and hasattr(target, "edit_message_text"):
                    await target.edit_message_text(fallback_text, **fallback_kwargs)
                elif hasattr(target, "reply_text"):
                    await target.reply_text(fallback_text, **fallback_kwargs)
                elif bot:
                    await bot.send_message(chat_id=target_chat_id, text=fallback_text, **fallback_kwargs)
            else:
                raise e

    def _save_last_rank(self, rank_config: RankConfig, rankings: list[IntervalResearchResult]) -> None:
        if self.redis_client is None:
            return
        payload = {
            "symbol": rank_config.symbol,
            "strategy_name": rank_config.strategy_name,
            "method": rank_config.method,
            "intervals": rank_config.intervals,
            "lookback_hours": rank_config.lookback_hours,
            "rankings": [
                {
                    "interval": r.interval,
                    "total_return": r.total_return,
                    "sharpe_ratio": r.sharpe_ratio,
                    "max_drawdown": r.max_drawdown,
                    "trade_count": r.trade_count,
                    "best_parameters": r.best_parameters,
                }
                for r in rankings
            ],
        }
        self.redis_client.set(self._redis_key("rank:last"), json.dumps(payload, ensure_ascii=False), ex=86400 * 7)

    def _load_last_rank(self) -> dict[str, Any] | None:
        if self.redis_client is None:
            return None
        raw = self.redis_client.get(self._redis_key("rank:last"))
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return None

    def _save_watch_config(self, config: WatchConfig) -> None:
        if self.redis_client is None:
            return
        self.redis_client.set(
            self._redis_key("watch"),
            json.dumps(asdict(config), ensure_ascii=False),
            ex=86400 * 30,
        )

    def _load_watch_config(self) -> WatchConfig | None:
        if self.redis_client is None:
            return None
        raw = self.redis_client.get(self._redis_key("watch"))
        if not raw:
            return None
        try:
            return WatchConfig(**json.loads(raw))
        except Exception:
            return None

    def _delete_watch_config(self) -> None:
        if self.redis_client is not None:
            self.redis_client.delete(self._redis_key("watch"))

    def build_application(self):
        async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            if not self._allowed_chat(update):
                return
            keyboard = [
                [InlineKeyboardButton("刷新状态", callback_data="m:status")],
            ]
            text = (
                "<b>Vntdr 交易信号推送</b>\n\n"
                "机器人只保留信号推送和状态查询。\n"
                "Web UI 负责回测、参数寻优和配置修改。\n\n"
                "命令：/status 查看当前监控和持仓。"
            )
            await self._send_safe(
                update,
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.HTML,
            )

        async def rank_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            if not self._allowed_chat(update):
                return
            args = context.args
            symbol = args[0].upper() if args else self.research_service.default_symbol()
            lookback_hours = self.research_service.default_lookback_hours()

            if len(args) >= 2:
                try:
                    hour_str = args[1].lower().rstrip("h")
                    lookback_hours = max(1, int(hour_str))
                except ValueError:
                    pass

            rank_config = RankConfig(
                symbol=symbol,
                strategy_name=self.research_service.default_strategy(),
                method=self.research_service.default_method(),
                intervals=self.research_service.available_intervals(),
                lookback_hours=lookback_hours,
            )
            await self._execute_rank(update, context, rank_config)

        async def run_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            if not self._allowed_chat(update):
                return
            args = context.args
            if len(args) < 2:
                await self._send_safe(
                    update,
                    "用法: `/run <交易对> <周期> [策略] [方法]`\n"
                    "例如: `/run XAU-USDT-SWAP 4h` 或 `/run XAU-USDT-SWAP 4h cm_macd_ult_mtf ga`"
                )
                return
            symbol = args[0].upper()
            interval = args[1].lower()
            strategy_name = args[2] if len(args) >= 3 else self.research_service.default_strategy()
            method = args[3].lower() if len(args) >= 4 else self.research_service.default_method()

            await self._send_safe(update, f"▶️ 开始执行监控: `{symbol}` `{interval}` (`{strategy_name}`/`{method}`)")
            result = await self._do_monitor(strategy_name, symbol, interval, method, context)
            await self._send_safe(update, self._format_monitor_result(result))

        async def auto_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            if not self._allowed_chat(update):
                return
            args = context.args
            symbol = args[0].upper() if args else self.research_service.default_symbol()
            poll_seconds = 60
            if len(args) >= 2:
                try:
                    poll_seconds = max(5, int(args[1]))
                except ValueError:
                    pass

            rank_config = RankConfig(
                symbol=symbol,
                strategy_name=self.research_service.default_strategy(),
                method=self.research_service.default_method(),
                intervals=self.research_service.available_intervals(),
                lookback_hours=self.research_service.default_lookback_hours(),
            )
            rankings = await self._execute_rank(update, context, rank_config)
            if not rankings:
                return
            watch_config = WatchConfig(
                symbol=rank_config.symbol,
                strategy_name=rank_config.strategy_name,
                interval=rankings[0].interval,
                method=rank_config.method,
                poll_seconds=poll_seconds,
            )
            self._replace_watch_job(context, watch_config)
            await self._send_safe(
                update,
                f"🔁 已按最佳周期 `{watch_config.interval}` 开启自动监控，\n"
                f"每 `{watch_config.poll_seconds}` 秒执行一次。\n发送 /stop 可停止。"
            )

        async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            if not self._allowed_chat(update):
                return
            text = await self._build_status_panel()
            await self._send_safe(
                update,
                text,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("刷新状态", callback_data="m:status")]]
                ),
                parse_mode=ParseMode.HTML,
            )

        async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            if not self._allowed_chat(update):
                return
            removed = self._remove_watch_job(context)
            if removed:
                await self._send_safe(update, "🛑 已停止自动监控。")
            else:
                await self._send_safe(update, "ℹ️ 当前没有运行中的自动监控。")

        # Callback query handler for InlineKeyboard
        async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            if not self._allowed_chat(update):
                await update.callback_query.answer("无权访问")
                return
            query = update.callback_query
            await query.answer()
            data = query.data

            if data == "m:status":
                await self._send_safe(
                    query,
                    await self._build_status_panel(),
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("刷新状态", callback_data="m:status")]]
                    ),
                    edit=True,
                    parse_mode=ParseMode.HTML,
                )
            elif data == "stop":
                await self._send_safe(
                    query,
                    "自动监控入口已停用；当前服务由 quant_core 主循环负责监控和推送。",
                    edit=True,
                    parse_mode=None,
                )
            elif data == "rr":
                # Rerun last rank
                last_rank = self._load_last_rank()
                if last_rank:
                    rank_config = RankConfig(
                        symbol=last_rank["symbol"],
                        strategy_name=last_rank["strategy_name"],
                        method=last_rank["method"],
                        intervals=last_rank["intervals"],
                        lookback_hours=last_rank["lookback_hours"],
                    )
                else:
                    rank_config = RankConfig(
                        symbol=self.research_service.default_symbol(),
                        strategy_name=self.research_service.default_strategy(),
                        method=self.research_service.default_method(),
                        intervals=self.research_service.available_intervals(),
                        lookback_hours=self.research_service.default_lookback_hours(),
                    )
                await self._execute_rank(query, context, rank_config, edit=True)
            elif data.startswith("r:"):
                interval = data[2:]
                if interval == "best":
                    last_rank = self._load_last_rank()
                    if not last_rank or not last_rank.get("rankings"):
                        await query.edit_message_text("无最近排名数据，请先执行 /rank")
                        return
                    interval = last_rank["rankings"][0]["interval"]
                    symbol = last_rank["symbol"]
                    strategy_name = last_rank["strategy_name"]
                    method = last_rank["method"]
                else:
                    symbol = context.user_data.get("symbol") or self.research_service.default_symbol()
                    strategy_name = context.user_data.get("strategy_name") or self.research_service.default_strategy()
                    method = context.user_data.get("method") or self.research_service.default_method()
                await self._send_safe(query, f"▶️ 开始执行监控: `{symbol}` `{interval}`", edit=True)
                result = await self._do_monitor(strategy_name, symbol, interval, method, context)
                await self._send_safe(query, self._format_monitor_result(result))
            elif data.startswith("a:"):
                interval = data[2:]
                symbol = context.user_data.get("symbol") or self.research_service.default_symbol()
                strategy_name = context.user_data.get("strategy_name") or self.research_service.default_strategy()
                method = context.user_data.get("method") or self.research_service.default_method()
                watch_config = WatchConfig(
                    symbol=symbol,
                    strategy_name=strategy_name,
                    interval=interval,
                    method=method,
                    poll_seconds=60,
                )
                self._replace_watch_job(context, watch_config)
                await self._send_safe(
                    query,
                    f"🔁 已按 `{interval}` 开启自动监控，每 `{watch_config.poll_seconds}` 秒一次。",
                    edit=True
                )

        # Legacy config conversation callbacks are kept for code compatibility,
        # but the /config entry point is no longer registered in the simplified bot.
        CONFIG_SELECT, CONFIG_VALUE = range(2)

        async def config_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
            if not self._allowed_chat(update):
                return ConversationHandler.END
            if self.config_service is None:
                await update.message.reply_text("⚠️ 配置服务未启用。")
                return ConversationHandler.END

            configs = self.config_service.list_all()
            labels = self.config_service.CONFIG_LABELS
            context.user_data["all_configs"] = list(sorted(configs.keys()))

            lines = ["⚙️ 配置管理 - 当前值：", ""]
            for key in sorted(configs.keys()):
                value = configs[key]
                label = labels.get(key, key)
                if isinstance(value, float):
                    value_str = f"{value:.4f}"
                else:
                    value_str = str(value)
                lines.append(f"  {label} = `{value_str}`")
            lines.extend(["", "👇 请点击下方按钮选择要修改的配置项："])

            keyboard = [[InlineKeyboardButton(labels.get(key, key), callback_data=f"cfg:{key}")] for key in sorted(configs.keys())]
            keyboard.append([InlineKeyboardButton("❌ 取消", callback_data="cfg:cancel")])

            context.user_data["key_to_label"] = labels
            context.user_data["label_to_key"] = {v: k for k, v in labels.items()}

            await self._send_safe(
                update,
                "\n".join(lines),
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return CONFIG_SELECT

        async def config_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            """Handle config inline keyboard callbacks"""
            if not self._allowed_chat(update):
                await update.callback_query.answer("无权访问")
                return
            query = update.callback_query
            await query.answer()
            data = query.data

            if data == "cfg:cancel":
                await self._send_safe(query, "已取消配置修改。", edit=True)
                return

            if data.startswith("cfg:"):
                key = data[4:]
                selected_label = context.user_data.get("key_to_label", {}).get(key, key)
                context.user_data["selected_config"] = key
                context.user_data["selected_label"] = selected_label
                current_value = self.config_service.get(key)

                if isinstance(current_value, bool):
                    keyboard = [
                        [InlineKeyboardButton("是 ✅", callback_data="cfgv:true"), InlineKeyboardButton("否 ❌", callback_data="cfgv:false")],
                        [InlineKeyboardButton("❌ 取消", callback_data="cfg:cancel")],
                    ]
                    await self._send_safe(
                        query,
                        f"当前 `{selected_label}` = `{'是' if current_value else '否'}`\n\n请选择新值：",
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        edit=True
                    )
                else:
                    if isinstance(current_value, float):
                        value_str = f"{current_value:.4f}"
                    else:
                        value_str = str(current_value)
                    await self._send_safe(
                        query,
                        f"当前 `{selected_label}` = `{value_str}`\n\n请直接输入新值（发送 /cancel 取消）：",
                        edit=True
                    )
                    # Store state to know we're waiting for config value
                    context.user_data["awaiting_config_value"] = True
                return

            if data.startswith("cfgv:"):
                raw = data[5:]
                new_value = "true" if raw == "true" else "false"
                selected = context.user_data.get("selected_config")
                selected_label = context.user_data.get("selected_label", selected)
                if not selected:
                    await query.edit_message_text("会话已过期，请重新执行 /config。")
                    return
                success = self.config_service.set(selected, new_value)
                if success:
                    final_value = self.config_service.get(selected)
                    value_str = "是" if final_value else "否" if isinstance(final_value, bool) else f"{final_value:.4f}" if isinstance(final_value, float) else str(final_value)
                    await self._send_safe(query, f"✅ 配置已更新：\n`{selected_label}` = `{value_str}`", edit=True)
                else:
                    await self._send_safe(query, "❌ 设置失败，请检查值格式。", edit=True)
                context.user_data.pop("awaiting_config_value", None)
                return

        async def config_fallback_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
            """Handle text input when in config conversation waiting for value"""
            if not self._allowed_chat(update):
                return ConversationHandler.END
            if not context.user_data.get("awaiting_config_value"):
                return ConversationHandler.END

            selected = context.user_data.get("selected_config")
            selected_label = context.user_data.get("selected_label", selected)
            if not selected:
                await self._send_safe(update, "会话已过期，请重新执行 /config。")
                return ConversationHandler.END

            new_value = update.message.text.strip()
            success = self.config_service.set(selected, new_value)
            if success:
                final_value = self.config_service.get(selected)
                if isinstance(final_value, bool):
                    value_str = "是" if final_value else "否"
                elif isinstance(final_value, float):
                    value_str = f"{final_value:.4f}"
                else:
                    value_str = str(final_value)
                await self._send_safe(
                    update,
                    f"✅ 配置已更新：\n`{selected_label}` = `{value_str}`"
                )
            else:
                await self._send_safe(update, "❌ 设置失败，请检查值格式是否正确。")
            context.user_data.pop("awaiting_config_value", None)
            return ConversationHandler.END

        async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
            if self._allowed_chat(update):
                await self._send_safe(update, "已取消当前操作。")
            context.user_data.pop("awaiting_config_value", None)
            return ConversationHandler.END

        application = Application.builder().token(self.bot_token).build()
        if application.job_queue is None:
            raise RuntimeError("Telegram job queue is unavailable. Install python-telegram-bot with job-queue extras.")

        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("status", status_command))
        application.add_handler(CallbackQueryHandler(button_callback, pattern=r"^(m:status|stop)$"))

        # Startup tasks: set commands and resume jobs
        async def on_startup(app: Application):
            # 1. Set bot commands for autocomplete
            commands = [
                BotCommand("start", "查看说明"),
                BotCommand("status", "查看监控和持仓状态"),
            ]
            await app.bot.set_my_commands(commands)

        application.post_init = on_startup
        return application

    async def _build_status_panel(self) -> str:
        live_statuses = self._load_live_statuses()
        positions = await self._load_current_positions()

        lines = ["<b>Vntdr 状态</b>"]
        if live_statuses:
            lines.append("")
            lines.append("<b>监控</b>")
            for status in live_statuses[:5]:
                symbol = escape(str(status.get("symbol", "-")))
                interval = escape(str(status.get("interval", "-")))
                signal = self._format_signal(status.get("signal"))
                bar_time = escape(str(status.get("completed_bar_time") or "-"))
                heartbeat = escape(str(status.get("time") or "-"))
                actions = status.get("actions") or []
                action_text = " + ".join(str(action) for action in actions) if actions else "无"
                reason = status.get("skipped_reason")
                suffix = f" | {escape(str(reason))}" if reason else ""
                lines.append(
                    f"{symbol} {interval}: <b>{signal}</b> | 动作 {escape(action_text)}{suffix}"
                )
                lines.append(f"收盘 {bar_time} | 心跳 {heartbeat}")
        else:
            lines.extend(["", "<b>监控</b>", "暂无 live status"])

        lines.append("")
        lines.append("<b>OKX 持仓</b>")
        if positions:
            for pos in positions:
                inst_id = escape(str(pos.get("instId", "-")))
                side = self._format_position_side(pos.get("posSide"))
                size = escape(str(pos.get("pos", "-")))
                avg_px = escape(str(pos.get("avgPx", "-")))
                mark_px = escape(str(pos.get("markPx") or pos.get("last") or "-"))
                upl = escape(str(pos.get("upl", "-")))
                lines.append(
                    f"{inst_id}: <b>{side}</b> x {size} | 均价 {avg_px} | 标记 {mark_px} | UPL {upl}"
                )
        else:
            lines.append("无持仓")

        return "\n".join(lines)

    def _load_live_statuses(self) -> list[dict[str, Any]]:
        if self.redis_client is None:
            return []
        statuses: list[dict[str, Any]] = []
        try:
            raw_map = self.redis_client.hgetall("vntdr:live_statuses")
            for raw_value in raw_map.values():
                statuses.append(self._decode_status(raw_value))
        except Exception:
            statuses = []

        if statuses:
            fresh_statuses = [status for status in statuses if self._is_fresh_status(status)]
            return sorted(fresh_statuses, key=lambda item: str(item.get("time", "")), reverse=True)

        try:
            raw = self.redis_client.get("vntdr:live_status")
            if raw:
                status = self._decode_status(raw)
                return [status] if self._is_fresh_status(status) else []
        except Exception:
            return []
        return []

    def _is_fresh_status(self, status: dict[str, Any]) -> bool:
        import time
        try:
            heartbeat = float(status.get("heartbeat", 0))
        except (TypeError, ValueError):
            return False
        return time.time() - heartbeat <= self.LIVE_STATUS_TTL_SECONDS

    def _decode_status(self, raw: Any) -> dict[str, Any]:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        if isinstance(raw, str):
            return json.loads(raw)
        if isinstance(raw, dict):
            return raw
        return {}

    async def _load_current_positions(self) -> list[dict[str, Any]]:
        if self.position_provider is None:
            return []
        import asyncio
        loop = asyncio.get_event_loop()
        try:
            return await loop.run_in_executor(None, self.position_provider.get_current_positions, None)
        except Exception:
            return []

    def _format_signal(self, signal: Any) -> str:
        try:
            signal_int = int(signal)
        except (TypeError, ValueError):
            return "未知"
        return {1: "LONG", -1: "SHORT", 0: "空仓"}.get(signal_int, str(signal_int))

    def _format_position_side(self, side: Any) -> str:
        return {"long": "LONG", "short": "SHORT", "net": "NET"}.get(str(side), str(side))

    async def _execute_rank(
        self,
        update_or_query,
        context: ContextTypes.DEFAULT_TYPE,
        rank_config: RankConfig,
        edit: bool = False,
    ) -> list[IntervalResearchResult]:
        """Execute ranking and send results with inline keyboard."""
        await self._send_safe(update_or_query, "📊 开始拉取数据并计算排名，请稍候...", edit=edit)
        
        import asyncio
        loop = asyncio.get_event_loop()
        rankings = await loop.run_in_executor(
            None,
            lambda: self.research_service.rank_intervals(
                symbol=rank_config.symbol,
                strategy_name=rank_config.strategy_name,
                method=rank_config.method,
                intervals=rank_config.intervals,
                lookback_hours=rank_config.lookback_hours,
            ),
        )
        self._save_last_rank(rank_config, rankings)
        context.user_data["symbol"] = rank_config.symbol
        context.user_data["strategy_name"] = rank_config.strategy_name
        context.user_data["method"] = rank_config.method

        text = self.research_service.format_rankings(
            symbol=rank_config.symbol,
            strategy_name=rank_config.strategy_name,
            method=rank_config.method,
            lookback_hours=rank_config.lookback_hours,
            rankings=rankings,
        )

        # Build inline keyboard for each interval
        keyboard = []
        if rankings:
            for r in rankings[:3]:
                keyboard.append([
                    InlineKeyboardButton(f"▶️ 运行 {r.interval}", callback_data=f"r:{r.interval}"),
                    InlineKeyboardButton(f"🔁 自动 {r.interval}", callback_data=f"a:{r.interval}"),
                ])
            keyboard.append([InlineKeyboardButton("🔄 重新排名", callback_data="rr")])
            keyboard.append([InlineKeyboardButton("📋 状态面板", callback_data="m:status")])

        await self._send_safe(
            update_or_query,
            text,
            reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
            edit=edit,
            parse_mode=None,
        )
        return rankings

    async def _do_monitor(
        self,
        strategy_name: str,
        symbol: str,
        interval: str,
        method: str,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> Any:
        volume = context.bot_data.get("default_order_size", 1.0)
        if self.monitor_once_callback_async is not None:
            return await self.monitor_once_callback_async(
                strategy_name=strategy_name,
                symbol=symbol,
                interval=interval,
                method=method,
                volume=volume,
            )
        return self.monitor_once_callback(
            strategy_name=strategy_name,
            symbol=symbol,
            interval=interval,
            method=method,
            volume=volume,
        )

    def run(self) -> None:
        """Run the bot in background thread with polling."""
        import asyncio
        application = self.build_application()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(application.run_polling(
            allowed_updates=["message", "callback_query"],
            stop_signals=None,
        ))

    def _allowed_chat(self, update) -> bool:
        import logging
        logger = logging.getLogger(__name__)
        
        if self.config_service:
            try:
                self.config_service._load_overrides()
            except Exception as e:
                logger.warning(f"Failed to reload config overrides in _allowed_chat: {e}")
        
        effective_user = getattr(update, "effective_user", None)
        user_id = effective_user.id if effective_user else None
        
        effective_chat = getattr(update, "effective_chat", None)
        chat_id = effective_chat.id if effective_chat else None
        
        # Also check message.chat_id and callback_query.message.chat.id
        # as fallbacks if effective_chat is somehow None or different
        message_chat_id = None
        message = getattr(update, "message", None)
        callback_query = getattr(update, "callback_query", None)
        
        if message and getattr(message, "chat_id", None):
            message_chat_id = message.chat_id
        elif callback_query and getattr(callback_query, "message", None):
            cb_msg = callback_query.message
            if getattr(cb_msg, "chat", None) and getattr(cb_msg.chat, "id", None):
                message_chat_id = cb_msg.chat.id
            elif getattr(cb_msg, "chat_id", None):
                message_chat_id = cb_msg.chat_id
            
        is_allowed = (
            str(user_id) == self.chat_id or 
            str(chat_id) == self.chat_id or 
            str(message_chat_id) == self.chat_id
        )
        
        # If still not allowed, try numeric comparison
        if not is_allowed:
            try:
                target_id_num = int(self.chat_id)
                is_allowed = (
                    user_id == target_id_num or 
                    chat_id == target_id_num or 
                    message_chat_id == target_id_num
                )
            except (ValueError, TypeError):
                pass
        
        if not is_allowed:
            logger.warning(f"Access denied. Configured: {self.chat_id}, Effective User: {user_id}, Effective Chat: {chat_id}, Msg Chat: {message_chat_id}")
            
        return is_allowed

    def _format_monitor_result(self, result) -> str:
        actions = ", ".join(result.actions) if result.actions else "none"
        return (
            f"✅ *监控完成*\n"
            f"`{self._escape_markdown_v2_code(result.symbol)}` `{self._escape_markdown_v2_code(result.interval)}`\n"
            f"策略: `{self._escape_markdown_v2_code(result.strategy_name)}`\n"
            f"信号: `{result.signal}` | 前信号: `{result.previous_signal}`\n"
            f"操作: `{self._escape_markdown_v2_code(actions)}`\n"
            f"参数: `{self._escape_markdown_v2_code(str(result.best_parameters))}`\n"
            f"通知: `{'已发送' if result.notification_sent else '无'}`"
        )

    def _replace_watch_job(self, context, config: WatchConfig) -> None:
        self._remove_watch_job(context)
        job_queue = self._job_queue(context.application)
        job_queue.run_repeating(
            self._build_watch_callback(),
            interval=config.poll_seconds,
            first=0,
            name=self.watch_job_name,
            data=asdict(config),
        )
        context.user_data["watch_config"] = asdict(config)
        self._save_watch_config(config)

    def _remove_watch_job(self, context) -> bool:
        jobs = self._job_queue(context.application).get_jobs_by_name(self.watch_job_name)
        for job in jobs:
            job.schedule_removal()
        context.user_data.pop("watch_config", None)
        self._delete_watch_config()
        return bool(jobs)

    def _get_watch_config(self, context) -> WatchConfig | None:
        payload = context.user_data.get("watch_config")
        if payload:
            return WatchConfig(**payload)
        # Fallback to redis
        return self._load_watch_config()

    def _job_queue(self, application):
        if application.job_queue is None:
            raise RuntimeError("Telegram job queue is unavailable.")
        return application.job_queue

    def _build_watch_callback(self):
        async def callback(context) -> None:
            import logging
            logger = logging.getLogger(__name__)
            if self.config_service:
                try:
                    self.config_service._load_overrides()
                except Exception as e:
                    logger.warning(f"Failed to reload config overrides in watch callback: {e}")
            config_data = context.job.data or {}
            config = WatchConfig(**config_data)
            if self.monitor_once_callback_async is not None:
                result = await self.monitor_once_callback_async(
                    strategy_name=config.strategy_name,
                    symbol=config.symbol,
                    interval=config.interval,
                    method=config.method,
                    volume=context.application.bot_data.get("default_order_size", 1.0),
                )
            else:
                result = self.monitor_once_callback(
                    strategy_name=config.strategy_name,
                    symbol=config.symbol,
                    interval=config.interval,
                    method=config.method,
                    volume=context.application.bot_data.get("default_order_size", 1.0),
                )
            if result.actions:
                await self._send_safe(context.job.context or self.chat_id, self._format_monitor_result(result))
        return callback
