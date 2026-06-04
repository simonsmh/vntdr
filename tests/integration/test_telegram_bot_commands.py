from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from telegram import InlineKeyboardMarkup

from vntdr.adapters.telegram_bot import RankConfig, TelegramCommandBot, WatchConfig
from vntdr.models import MonitorResult


@dataclass
class FakeRanking:
    interval: str
    total_return: float
    sharpe_ratio: float
    max_drawdown: float
    trade_count: float
    best_parameters: dict[str, Any] = field(default_factory=dict)
    sync_inserted_count: int = 0


@dataclass
class FakeResearchService:
    default_symbol_name: str = "XAU-USDT-SWAP"
    default_strategy_name: str = "cm_macd_ult_mtf"
    default_method_name: str = "ga"

    def default_symbol(self) -> str:
        return self.default_symbol_name

    def default_strategy(self) -> str:
        return self.default_strategy_name

    def default_method(self) -> str:
        return self.default_method_name

    def default_ranking_intervals(self) -> list[str]:
        return ["15m", "1h", "4h"]

    def default_lookback_hours(self) -> int:
        return 24

    def available_intervals(self) -> list[str]:
        return ["15m", "1h", "4h"]

    def available_strategies(self) -> list[str]:
        return ["cm_macd_ult_mtf"]

    def available_methods(self) -> list[str]:
        return ["ga"]

    def rank_intervals(
        self,
        *,
        symbol: str,
        strategy_name: str,
        method: str,
        intervals: list[str],
        lookback_hours: int,
    ) -> list[FakeRanking]:
        return [
            FakeRanking(
                interval="4h",
                total_return=0.155,
                sharpe_ratio=1.8,
                max_drawdown=0.082,
                trade_count=20,
                best_parameters={"fast_length": 3},
            ),
            FakeRanking(
                interval="1h",
                total_return=0.123,
                sharpe_ratio=1.5,
                max_drawdown=0.095,
                trade_count=50,
                best_parameters={"fast_length": 5},
            ),
        ]

    def format_rankings(
        self,
        *,
        symbol: str,
        strategy_name: str,
        method: str,
        lookback_hours: int,
        rankings: list[Any],
    ) -> str:
        lines = [f"排名结果: {symbol} {strategy_name}", ""]
        for i, r in enumerate(rankings, 1):
            lines.append(f"{i}. {r.interval}: 收益={r.total_return * 100:.1f}%")
        return "\n".join(lines)


class FakeRedis:
    """In-memory Redis mock"""

    def __init__(self) -> None:
        self._store: dict[str, str] = {}

    def set(self, key: str, value: str, ex=None) -> None:
        self._store[key] = value

    def get(self, key: str) -> str | None:
        return self._store.get(key)

    def delete(self, key: str) -> None:
        self._store.pop(key, None)


@pytest.fixture
def fake_research_service():
    return FakeResearchService()


@pytest.fixture
def fake_redis():
    return FakeRedis()


@pytest.fixture
def bot(fake_research_service, fake_redis):
    def monitor_callback(**kwargs):
        return MonitorResult(
            symbol=kwargs.get("symbol", "XAU-USDT-SWAP"),
            interval=kwargs.get("interval", "4h"),
            strategy_name=kwargs.get("strategy_name", "cm_macd_ult_mtf"),
            signal=1,
            previous_signal=0,
            best_parameters={"fast_length": 3},
            actions=["OPEN_LONG"],
            notification_sent=True,
        )

    return TelegramCommandBot(
        bot_token="test-token",
        chat_id="12345",
        research_service=fake_research_service,
        monitor_once_callback=monitor_callback,
        redis_client=fake_redis,
    )


def _make_mock_message(text: str, chat_id: int = 12345):
    """Create a fully mock Message that allows attribute assignment."""
    msg = MagicMock()
    msg.text = text
    msg.chat_id = chat_id
    msg.chat = MagicMock()
    msg.chat.id = chat_id
    msg.from_user = MagicMock()
    msg.from_user.id = 1
    msg.date = datetime.now(tz=timezone.utc)
    msg.message_id = 1
    return msg


def _make_update(text: str, chat_id: int = 12345):
    update = MagicMock()
    update.message = _make_mock_message(text, chat_id)
    update.callback_query = None
    return update


def _make_callback_update(data: str, chat_id: int = 12345):
    update = MagicMock()
    update.message = None
    query = MagicMock()
    query.data = data
    query.chat_instance = "ci"
    query.from_user = MagicMock()
    query.from_user.id = 1
    query.message = _make_mock_message("callback", chat_id)
    query.chat_id = chat_id
    query.answer = AsyncMock()
    update.callback_query = query
    return update


class AsyncContext:
    """Mock ContextTypes.DEFAULT_TYPE"""

    def __init__(self, chat_id: int = 12345, args: list[str] | None = None):
        self.user_data: dict[str, Any] = {}
        self.bot_data: dict[str, Any] = {"default_order_size": 1.0}
        self._chat_id = chat_id
        self.application = MagicMock()
        self.application.job_queue = MagicMock()
        self.args = args or []

    async def bot_send_message(self, **kwargs):
        pass


class TestTelegramBotCommands:
    """Telegram Bot 命令集成测试"""

    def _find_handler(self, application, command_name: str | None = None, handler_type: str | None = None):
        """Find handler by command name or type."""
        for group in application.handlers.values():
            for handler in group:
                if command_name:
                    cmds = getattr(handler, "commands", set())
                    if command_name in cmds:
                        return handler
                if handler_type and type(handler).__name__ == handler_type:
                    return handler
        return None

    @pytest.mark.asyncio
    async def test_start_command_sends_menu(self, bot):
        update = _make_update("/start")
        sent = []

        async def mock_reply_text(text, **kwargs):
            sent.append({"text": text, "reply_markup": kwargs.get("reply_markup")})

        update.message.reply_text = mock_reply_text
        context = AsyncContext()
        application = bot.build_application()
        handler = self._find_handler(application, "start")
        assert handler is not None
        await handler.callback(update, context)

        assert len(sent) == 1
        assert "Vntdr 量化交易机器人" in sent[0]["text"]
        assert sent[0]["reply_markup"] is not None
        assert isinstance(sent[0]["reply_markup"], InlineKeyboardMarkup)

    @pytest.mark.asyncio
    async def test_rank_command_with_symbol(self, bot):
        update = _make_update("/rank XAU-USDT-SWAP")
        sent = []

        async def mock_reply_text(text, **kwargs):
            sent.append({"text": text, "reply_markup": kwargs.get("reply_markup")})

        update.message.reply_text = mock_reply_text
        context = AsyncContext(args=["XAU-USDT-SWAP"])
        application = bot.build_application()
        handler = self._find_handler(application, "rank")
        assert handler is not None
        await handler.callback(update, context)

        assert len(sent) >= 2
        assert "开始拉取数据" in sent[0]["text"]
        assert "排名结果" in sent[-1]["text"]
        assert "XAU-USDT-SWAP" in sent[-1]["text"]
        assert sent[-1]["reply_markup"] is not None
        assert isinstance(sent[-1]["reply_markup"], InlineKeyboardMarkup)

    @pytest.mark.asyncio
    async def test_run_command_with_args(self, bot):
        update = _make_update("/run XAU-USDT-SWAP 4h")
        sent = []

        async def mock_reply_text(text, **kwargs):
            sent.append({"text": text})

        update.message.reply_text = mock_reply_text
        context = AsyncContext(args=["XAU-USDT-SWAP", "4h"])
        application = bot.build_application()
        handler = self._find_handler(application, "run")
        assert handler is not None
        await handler.callback(update, context)

        assert len(sent) >= 2
        assert "开始执行监控" in sent[0]["text"]
        assert "XAU-USDT-SWAP" in sent[0]["text"]
        assert "4h" in sent[0]["text"]

    @pytest.mark.asyncio
    async def test_run_command_without_enough_args(self, bot):
        update = _make_update("/run XAU-USDT-SWAP")
        sent = []

        async def mock_reply_text(text, **kwargs):
            sent.append({"text": text})

        update.message.reply_text = mock_reply_text
        context = AsyncContext(args=["XAU-USDT-SWAP"])
        application = bot.build_application()
        handler = self._find_handler(application, "run")
        assert handler is not None
        await handler.callback(update, context)

        assert len(sent) == 1
        assert "用法" in sent[0]["text"]

    @pytest.mark.asyncio
    async def test_stop_command_when_no_watch(self, bot):
        update = _make_update("/stop")
        sent = []

        async def mock_reply_text(text, **kwargs):
            sent.append({"text": text})

        update.message.reply_text = mock_reply_text
        context = AsyncContext()
        # Make job_queue.get_jobs_by_name return empty list so no watch is found
        context.application.job_queue.get_jobs_by_name = lambda name: []
        application = bot.build_application()
        handler = self._find_handler(application, "stop")
        assert handler is not None
        await handler.callback(update, context)

        assert len(sent) == 1
        assert ("没有运行" in sent[0]["text"] or "未运行" in sent[0]["text"])

    @pytest.mark.asyncio
    async def test_status_command_shows_panel(self, bot):
        update = _make_update("/status")
        sent = []

        async def mock_reply_text(text, **kwargs):
            sent.append({"text": text, "reply_markup": kwargs.get("reply_markup")})

        update.message.reply_text = mock_reply_text
        context = AsyncContext()
        # Ensure no watch jobs exist
        context.application.job_queue.get_jobs_by_name = lambda name: []
        application = bot.build_application()
        handler = self._find_handler(application, "status")
        assert handler is not None
        await handler.callback(update, context)

        assert len(sent) == 1
        assert "状态面板" in sent[0]["text"]
        assert sent[0]["reply_markup"] is not None

    @pytest.mark.asyncio
    async def test_inline_callback_menu_rank(self, bot):
        update = _make_callback_update("m:rank")
        edited = []

        async def mock_edit_message_text(text, **kwargs):
            edited.append({"text": text, "reply_markup": kwargs.get("reply_markup")})

        update.callback_query.edit_message_text = mock_edit_message_text
        context = AsyncContext()
        application = bot.build_application()
        handler = self._find_handler(application, handler_type="CallbackQueryHandler")
        assert handler is not None
        await handler.callback(update, context)

        assert len(edited) >= 1
        assert "排名结果" in edited[-1]["text"]

    @pytest.mark.asyncio
    async def test_inline_callback_stop(self, bot):
        update = _make_callback_update("stop")
        edited = []

        async def mock_edit_message_text(text, **kwargs):
            edited.append({"text": text})

        update.callback_query.edit_message_text = mock_edit_message_text
        context = AsyncContext()
        # No active jobs
        context.application.job_queue.get_jobs_by_name = lambda name: []
        application = bot.build_application()
        handler = self._find_handler(application, handler_type="CallbackQueryHandler")
        assert handler is not None
        await handler.callback(update, context)

        assert len(edited) == 1
        assert ("停止" in edited[0]["text"] or "没有运行" in edited[0]["text"])

    @pytest.mark.asyncio
    async def test_rank_redis_persistence(self, bot, fake_redis):
        update = _make_update("/rank XAU-USDT-SWAP")
        sent = []

        async def mock_reply_text(text, **kwargs):
            sent.append({"text": text})

        update.message.reply_text = mock_reply_text
        context = AsyncContext(args=["XAU-USDT-SWAP"])
        application = bot.build_application()
        handler = self._find_handler(application, "rank")
        assert handler is not None
        await handler.callback(update, context)

        # Verify Redis has stored the rank result
        key = bot._redis_key("rank:last")
        raw = fake_redis.get(key)
        assert raw is not None
        data = json.loads(raw)
        assert data["symbol"] == "XAU-USDT-SWAP"
        assert len(data["rankings"]) > 0

    def test_allowed_chat_matches(self, bot):
        class FakeMessage:
            chat_id = 12345

        class FakeUpdate:
            message = FakeMessage()

        assert bot._allowed_chat(FakeUpdate()) is True

        class FakeUpdate2:
            message = type("M", (), {"chat_id": 99999})()

        assert bot._allowed_chat(FakeUpdate2()) is False

    def test_watch_job_management(self, bot):
        context = AsyncContext()
        config = WatchConfig(
            symbol="XAU-USDT-SWAP",
            strategy_name="cm_macd_ult_mtf",
            interval="4h",
            method="ga",
            poll_seconds=60,
        )

        jobs = []

        class FakeJob:
            def __init__(self, name):
                self.name = name

            def schedule_removal(self):
                jobs.remove(self)

        class FakeJobQueue:
            def run_repeating(self, callback, interval, first, name, data):
                job = FakeJob(name)
                jobs.append(job)
                return job

            def get_jobs_by_name(self, name):
                return [j for j in jobs if j.name == name]

        context.application.job_queue = FakeJobQueue()

        bot._replace_watch_job(context, config)
        assert len(jobs) == 1
        assert context.user_data.get("watch_config") is not None

        removed = bot._remove_watch_job(context)
        assert removed is True
        assert len(jobs) == 0
        assert context.user_data.get("watch_config") is None

    def test_monitor_formatting(self, bot):
        result = MonitorResult(
            symbol="XAU-USDT-SWAP",
            interval="4h",
            strategy_name="cm_macd_ult_mtf",
            signal=1,
            previous_signal=0,
            best_parameters={"fast_length": 3},
            actions=["OPEN_LONG"],
            notification_sent=True,
        )
        text = bot._format_monitor_result(result)
        assert "XAU-USDT-SWAP" in text
        assert "4h" in text
        assert "OPEN_LONG" in text
        assert "fast_length" in text


if __name__ == "__main__":
    import sys

    result = pytest.main([
        __file__,
        "-v",
        "--tb=short",
    ])
    sys.exit(result)
