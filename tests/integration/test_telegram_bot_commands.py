from __future__ import annotations

import json
import time
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

    def hgetall(self, key: str) -> dict[str, str]:
        return {}


class FakePositionProvider:
    def get_current_positions(self, symbol: str | None = None) -> list[dict[str, str]]:
        return [
            {
                "instId": symbol or "XAU-USDT-SWAP",
                "posSide": "long",
                "pos": "1",
                "avgPx": "4335.7",
                "markPx": "4345.9",
                "upl": "0.0102",
            }
        ]


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
        assert "Vntdr 交易信号推送" in sent[0]["text"]
        assert sent[0]["reply_markup"] is not None
        assert isinstance(sent[0]["reply_markup"], InlineKeyboardMarkup)

    def test_legacy_interactive_commands_are_not_registered(self, bot):
        application = bot.build_application()
        assert self._find_handler(application, "rank") is None
        assert self._find_handler(application, "run") is None
        assert self._find_handler(application, "auto") is None
        assert self._find_handler(application, "config") is None
        assert self._find_handler(application, "stop") is None

    @pytest.mark.asyncio
    async def test_status_command_shows_panel(self, bot):
        update = _make_update("/status")
        sent = []

        async def mock_reply_text(text, **kwargs):
            sent.append({"text": text, "reply_markup": kwargs.get("reply_markup")})

        update.message.reply_text = mock_reply_text
        context = AsyncContext()
        bot.redis_client.set(
            "vntdr:live_status",
            json.dumps(
                {
                    "time": "2026-06-16 10:34:12 UTC",
                    "symbol": "XAU-USDT-SWAP",
                    "interval": "1h",
                    "signal": 1,
                    "actions": [],
                    "completed_bar_time": "2026-06-16T09:00:00+00:00",
                    "heartbeat": time.time(),
                }
            ),
        )
        bot.position_provider = FakePositionProvider()
        application = bot.build_application()
        handler = self._find_handler(application, "status")
        assert handler is not None
        await handler.callback(update, context)

        assert len(sent) == 1
        assert "Vntdr 状态" in sent[0]["text"]
        assert "XAU-USDT-SWAP 1h" in sent[0]["text"]
        assert "LONG" in sent[0]["text"]
        assert "OKX 持仓" in sent[0]["text"]
        assert "均价 4335.7" in sent[0]["text"]
        assert sent[0]["reply_markup"] is not None

    @pytest.mark.asyncio
    async def test_inline_callback_refreshes_status(self, bot):
        update = _make_callback_update("m:status")
        edited = []

        async def mock_edit_message_text(text, **kwargs):
            edited.append({"text": text, "reply_markup": kwargs.get("reply_markup")})

        update.callback_query.edit_message_text = mock_edit_message_text
        context = AsyncContext()
        application = bot.build_application()
        handler = self._find_handler(application, handler_type="CallbackQueryHandler")
        assert handler is not None
        await handler.callback(update, context)

        assert len(edited) == 1
        assert "Vntdr 状态" in edited[0]["text"]

    def test_live_status_filters_stale_entries(self, bot):
        fresh = {
            "time": "2026-06-17 00:10:57 UTC",
            "symbol": "XAU-USDT-SWAP",
            "interval": "1h",
            "signal": 0,
            "heartbeat": time.time(),
        }
        stale = {
            "time": "2026-06-03 23:39:42 UTC",
            "symbol": "XAU-USDT-SWAP",
            "interval": "4h",
            "signal": 0,
            "heartbeat": 1780529982.446046,
        }
        bot.redis_client.hgetall = lambda key: {
            "fresh": json.dumps(fresh),
            "stale": json.dumps(stale),
        }

        statuses = bot._load_live_statuses()

        assert [status["interval"] for status in statuses] == ["1h"]

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
