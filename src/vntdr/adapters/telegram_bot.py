from __future__ import annotations

from dataclasses import asdict, dataclass
from html import escape
from typing import Final

from vntdr.services.telegram_research import TelegramResearchService

SYMBOL: Final[int] = 0
STRATEGY: Final[int] = 1
METHOD: Final[int] = 2
INTERVALS: Final[int] = 3
LOOKBACK: Final[int] = 4
MONITOR_INTERVAL: Final[int] = 5
WATCH_INTERVAL: Final[int] = 6


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
    def __init__(
        self,
        *,
        bot_token: str,
        chat_id: str,
        research_service: TelegramResearchService,
        monitor_once_callback,
    ) -> None:
        self.bot_token = bot_token
        self.chat_id = str(chat_id)
        self.research_service = research_service
        self.monitor_once_callback = monitor_once_callback
        self.watch_job_name = f"watch:{self.chat_id}"

    def build_application(self):
        from telegram import KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove, Update
        from telegram.ext import (
            Application,
            CommandHandler,
            ContextTypes,
            ConversationHandler,
            MessageHandler,
            filters,
        )

        async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            if not self._allowed_chat(update):
                return
            await update.message.reply_text(
                "发送 /rank 开始交互式回测排序，/monitor 执行一次监控，/watch 开启持续监控，/watch_top 自动监控第一名周期。"
            )

        async def rank_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
            if not self._allowed_chat(update):
                return ConversationHandler.END
            args = context.args
            if args:
                context.user_data["symbol"] = args[0].upper()
                return await ask_strategy(update, context)
            await update.message.reply_text("请输入交易品种，例如 XAUUSDT。")
            return SYMBOL

        async def receive_symbol(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
            context.user_data["symbol"] = update.message.text.strip().upper()
            return await ask_strategy(update, context)

        async def ask_strategy(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
            strategies = self.research_service.available_strategies()
            keyboard = [[KeyboardButton(name)] for name in strategies]
            await update.message.reply_text(
                "选择策略。",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True),
            )
            return STRATEGY

        async def receive_strategy(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
            context.user_data["strategy_name"] = update.message.text.strip()
            methods = self.research_service.available_methods()
            keyboard = [[KeyboardButton(name)] for name in methods]
            await update.message.reply_text(
                "选择优化方法。",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True),
            )
            return METHOD

        async def receive_method(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
            context.user_data["method"] = update.message.text.strip().lower()
            await update.message.reply_text(
                "请输入时间周期，多个用逗号分隔，例如 15m,30m,1h,4h。",
                reply_markup=ReplyKeyboardRemove(),
            )
            return INTERVALS

        async def receive_intervals(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
            raw = update.message.text.strip()
            context.user_data["intervals"] = [part.strip().lower() for part in raw.split(",") if part.strip()]
            await update.message.reply_text("请输入回看小时数，例如 24。")
            return LOOKBACK

        async def receive_lookback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
            lookback_hours = int(update.message.text.strip())
            rank_config = RankConfig(
                symbol=str(context.user_data["symbol"]),
                strategy_name=str(context.user_data["strategy_name"]),
                method=str(context.user_data["method"]),
                intervals=list(context.user_data["intervals"]),
                lookback_hours=lookback_hours,
            )
            rankings = await self._run_ranking(update, context, rank_config)
            if rankings:
                await update.message.reply_text(
                    f"当前最佳周期为 {rankings[0].interval}。发送 /monitor 可按该配置执行一次监控和下单。"
                )
            return ConversationHandler.END

        async def monitor_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
            if not self._allowed_chat(update):
                return ConversationHandler.END
            if not context.user_data.get("symbol") or not context.user_data.get("strategy_name"):
                await update.message.reply_text("请先发送 /rank 完成一次品种与策略选择。")
                return ConversationHandler.END
            keyboard = [[KeyboardButton(interval)] for interval in self.research_service.available_intervals()]
            await update.message.reply_text(
                "选择要监控的周期，默认建议使用刚才排名第一的周期。",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True),
            )
            return MONITOR_INTERVAL

        async def receive_monitor_interval(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
            interval = update.message.text.strip().lower()
            symbol = str(context.user_data["symbol"])
            strategy_name = str(context.user_data["strategy_name"])
            method = str(context.user_data.get("method", "grid"))
            await update.message.reply_text("开始执行监控，请稍候。", reply_markup=ReplyKeyboardRemove())
            result = self.monitor_once_callback(
                strategy_name=strategy_name,
                symbol=symbol,
                interval=interval,
                method=method,
            )
            context.user_data["monitor_interval"] = interval
            await update.message.reply_text(self._format_monitor_result(result))
            return ConversationHandler.END

        async def watch_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
            if not self._allowed_chat(update):
                return ConversationHandler.END
            if not context.user_data.get("symbol") or not context.user_data.get("strategy_name"):
                await update.message.reply_text("请先发送 /rank 完成一次品种与策略选择。")
                return ConversationHandler.END
            keyboard = [[KeyboardButton(interval)] for interval in self.research_service.available_intervals()]
            await update.message.reply_text(
                "选择持续监控的周期。",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True),
            )
            return WATCH_INTERVAL

        async def watch_top_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
            if not self._allowed_chat(update):
                return ConversationHandler.END
            args = context.args
            if len(args) >= 2:
                context.user_data["watch_top_symbol"] = args[0].upper()
                context.user_data["watch_top_poll_seconds"] = max(5, int(args[1]))
                return await run_watch_top(update, context)
            if len(args) == 1:
                raw = args[0]
                if raw.isdigit():
                    context.user_data["watch_top_poll_seconds"] = max(5, int(raw))
                else:
                    context.user_data["watch_top_symbol"] = raw.upper()
            return await run_watch_top(update, context)

        async def receive_watch_interval(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
            interval = update.message.text.strip().lower()
            context.user_data["monitor_interval"] = interval
            await update.message.reply_text(
                "请输入轮询秒数，例如 60。",
                reply_markup=ReplyKeyboardRemove(),
            )
            return LOOKBACK

        async def receive_watch_seconds(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
            poll_seconds = max(5, int(update.message.text.strip()))
            config = WatchConfig(
                symbol=str(context.user_data["symbol"]),
                strategy_name=str(context.user_data["strategy_name"]),
                interval=str(context.user_data["monitor_interval"]),
                method=str(context.user_data.get("method", "grid")),
                poll_seconds=poll_seconds,
            )
            self._replace_watch_job(context, config)
            await update.message.reply_text(
                f"已开启持续监控: {config.symbol} {config.strategy_name} {config.interval} 每 {config.poll_seconds} 秒一次。"
            )
            return ConversationHandler.END

        async def run_watch_top(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
            rank_config = self._resolve_watch_top_rank_config(context)
            rankings = await self._run_ranking(update, context, rank_config)
            if not rankings:
                await update.message.reply_text("没有得到可用的周期排名，未开启持续监控。")
                return ConversationHandler.END
            poll_seconds = int(context.user_data.get("watch_top_poll_seconds", 60))
            watch_config = WatchConfig(
                symbol=rank_config.symbol,
                strategy_name=rank_config.strategy_name,
                interval=rankings[0].interval,
                method=rank_config.method,
                poll_seconds=poll_seconds,
            )
            self._replace_watch_job(context, watch_config)
            await update.message.reply_text(
                f"已按第一名周期 {watch_config.interval} 开启持续监控，每 {watch_config.poll_seconds} 秒一次。"
            )
            return ConversationHandler.END

        async def watch_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            if not self._allowed_chat(update):
                return
            config = self._get_watch_config(context)
            if config is None:
                await update.message.reply_text("当前没有运行中的持续监控。")
                return
            await update.message.reply_text(self._format_watch_status(config))

        async def watch_stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            if not self._allowed_chat(update):
                return
            removed = self._remove_watch_job(context)
            if removed:
                await update.message.reply_text("已停止持续监控。")
            else:
                await update.message.reply_text("当前没有运行中的持续监控。")

        async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
            if self._allowed_chat(update):
                await update.message.reply_text("已取消当前回测会话。", reply_markup=ReplyKeyboardRemove())
            return ConversationHandler.END

        application = Application.builder().token(self.bot_token).build()
        if application.job_queue is None:
            raise RuntimeError("Telegram job queue is unavailable. Install python-telegram-bot with job-queue extras.")
        rank_conversation = ConversationHandler(
            entry_points=[CommandHandler("rank", rank_entry)],
            states={
                SYMBOL: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_symbol)],
                STRATEGY: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_strategy)],
                METHOD: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_method)],
                INTERVALS: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_intervals)],
                LOOKBACK: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_lookback)],
            },
            fallbacks=[CommandHandler("cancel", cancel)],
        )
        monitor_conversation = ConversationHandler(
            entry_points=[CommandHandler("monitor", monitor_entry)],
            states={
                MONITOR_INTERVAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_monitor_interval)],
            },
            fallbacks=[CommandHandler("cancel", cancel)],
        )
        watch_conversation = ConversationHandler(
            entry_points=[CommandHandler("watch", watch_entry)],
            states={
                WATCH_INTERVAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_watch_interval)],
                LOOKBACK: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_watch_seconds)],
            },
            fallbacks=[CommandHandler("cancel", cancel)],
        )
        application.add_handler(CommandHandler("start", start))
        application.add_handler(rank_conversation)
        application.add_handler(monitor_conversation)
        application.add_handler(watch_conversation)
        application.add_handler(CommandHandler("watch_top", watch_top_entry))
        application.add_handler(CommandHandler("watch_status", watch_status))
        application.add_handler(CommandHandler("watch_stop", watch_stop))
        return application

    def run(self) -> None:
        application = self.build_application()
        application.run_polling(allowed_updates=["message"])

    def _allowed_chat(self, update) -> bool:
        message = getattr(update, "message", None)
        if message is None or message.chat_id is None:
            return False
        return str(message.chat_id) == self.chat_id

    def _format_monitor_result(self, result) -> str:
        actions = ", ".join(result.actions) if result.actions else "none"
        return (
            f"monitor {escape(result.symbol)} {escape(result.interval)}\n"
            f"strategy={escape(result.strategy_name)} signal={result.signal} previous={result.previous_signal}\n"
            f"actions={actions}\n"
            f"parameters={result.best_parameters}\n"
            f"notified={result.notification_sent}"
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

    def _remove_watch_job(self, context) -> bool:
        jobs = self._job_queue(context.application).get_jobs_by_name(self.watch_job_name)
        for job in jobs:
            job.schedule_removal()
        context.user_data.pop("watch_config", None)
        return bool(jobs)

    def _get_watch_config(self, context) -> WatchConfig | None:
        payload = context.user_data.get("watch_config")
        if not payload:
            return None
        return WatchConfig(**payload)

    def _format_watch_status(self, config: WatchConfig) -> str:
        return (
            f"watching {config.symbol} {config.strategy_name} {config.interval}\n"
            f"method={config.method} every={config.poll_seconds}s"
        )

    async def _run_ranking(self, update, context, rank_config: RankConfig):
        self._store_rank_config(context, rank_config)
        await update.message.reply_text("开始拉取数据并计算排名，请稍候。", reply_markup=self._reply_keyboard_remove())
        rankings = self.research_service.rank_intervals(
            symbol=rank_config.symbol,
            strategy_name=rank_config.strategy_name,
            method=rank_config.method,
            intervals=rank_config.intervals,
            lookback_hours=rank_config.lookback_hours,
        )
        await update.message.reply_text(
            self.research_service.format_rankings(
                symbol=rank_config.symbol,
                strategy_name=rank_config.strategy_name,
                method=rank_config.method,
                lookback_hours=rank_config.lookback_hours,
                rankings=rankings,
            )
        )
        if rankings:
            context.user_data["monitor_interval"] = rankings[0].interval
            context.user_data["selected_parameters"] = rankings[0].best_parameters
        return rankings

    def _resolve_watch_top_rank_config(self, context) -> RankConfig:
        existing = self._get_rank_config(context)
        symbol = str(context.user_data.get("watch_top_symbol") or (existing.symbol if existing else self.research_service.default_symbol()))
        if existing is not None:
            return RankConfig(
                symbol=symbol,
                strategy_name=existing.strategy_name,
                method=existing.method,
                intervals=existing.intervals,
                lookback_hours=existing.lookback_hours,
            )
        return RankConfig(
            symbol=symbol,
            strategy_name=self.research_service.default_strategy(),
            method=self.research_service.default_method(),
            intervals=self.research_service.default_ranking_intervals(),
            lookback_hours=self.research_service.default_lookback_hours(),
        )

    def _store_rank_config(self, context, rank_config: RankConfig) -> None:
        context.user_data["symbol"] = rank_config.symbol
        context.user_data["strategy_name"] = rank_config.strategy_name
        context.user_data["method"] = rank_config.method
        context.user_data["intervals"] = rank_config.intervals
        context.user_data["lookback_hours"] = rank_config.lookback_hours

    def _get_rank_config(self, context) -> RankConfig | None:
        if not context.user_data.get("symbol") or not context.user_data.get("strategy_name"):
            return None
        return RankConfig(
            symbol=str(context.user_data["symbol"]),
            strategy_name=str(context.user_data["strategy_name"]),
            method=str(context.user_data.get("method", self.research_service.default_method())),
            intervals=list(context.user_data.get("intervals", self.research_service.default_ranking_intervals())),
            lookback_hours=int(context.user_data.get("lookback_hours", self.research_service.default_lookback_hours())),
        )

    def _job_queue(self, application):
        if application.job_queue is None:
            raise RuntimeError("Telegram job queue is unavailable.")
        return application.job_queue

    def _reply_keyboard_remove(self):
        from telegram import ReplyKeyboardRemove

        return ReplyKeyboardRemove()

    def _build_watch_callback(self):
        async def callback(context) -> None:
            config_data = context.job.data or {}
            config = WatchConfig(**config_data)
            result = self.monitor_once_callback(
                strategy_name=config.strategy_name,
                symbol=config.symbol,
                interval=config.interval,
                method=config.method,
            )
            if result.notification_sent or result.actions:
                await context.bot.send_message(chat_id=self.chat_id, text=self._format_monitor_result(result))

        return callback
