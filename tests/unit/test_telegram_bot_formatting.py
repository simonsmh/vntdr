from __future__ import annotations
from unittest.mock import AsyncMock, MagicMock

import pytest

from vntdr.adapters.telegram_bot import RankConfig, TelegramCommandBot, WatchConfig
from vntdr.models import MonitorResult
from vntdr.services.telegram_research import IntervalResearchResult


class DummyResearchService:
    def default_symbol(self) -> str:
        return "XAUUSDT"

    def default_strategy(self) -> str:
        return "cm_macd_ult_mtf"

    def default_method(self) -> str:
        return "ga"

    def default_ranking_intervals(self) -> list[str]:
        return ["15m", "30m", "1h", "4h"]

    def default_lookback_hours(self) -> int:
        return 24

    def available_intervals(self) -> list[str]:
        return ["15m", "30m", "1h", "4h"]

    def available_strategies(self) -> list[str]:
        return ["cm_macd_ult_mtf"]

    def available_methods(self) -> list[str]:
        return ["ga"]

    def rank_intervals(self, **kwargs) -> list:
        return []

    def format_rankings(self, **kwargs) -> str:
        return ""


def test_format_monitor_result_includes_actions_and_parameters() -> None:
    bot = TelegramCommandBot(
        bot_token="bot",
        chat_id="chat",
        research_service=DummyResearchService(),
        monitor_once_callback=lambda **_: None,
    )

    message = bot._format_monitor_result(
        MonitorResult(
            symbol="XAUUSDT",
            interval="15m",
            strategy_name="cm_macd_ult_mtf",
            signal=1,
            previous_signal=-1,
            best_parameters={"fast_length": 3},
            actions=["buy_short", "buy_long"],
            notification_sent=True,
        )
    )

    assert "XAUUSDT" in message
    assert "15m" in message
    assert "buy_short" in message
    assert "fast_length" in message


def test_format_watch_status_includes_poll_seconds() -> None:
    bot = TelegramCommandBot(
        bot_token="bot",
        chat_id="chat",
        research_service=DummyResearchService(),
        monitor_once_callback=lambda **_: None,
    )

    # Status panel should reference watch config
    watch = WatchConfig(
        symbol="XAUUSDT",
        strategy_name="cm_macd_ult_mtf",
        interval="15m",
        method="ga",
        poll_seconds=60,
    )

    assert watch.symbol == "XAUUSDT"
    assert watch.interval == "15m"
    assert watch.poll_seconds == 60


def test_rank_config_creation() -> None:
    config = RankConfig(
        symbol="BTC-USDT-SWAP",
        strategy_name="cm_macd_ult_mtf",
        method="ga",
        intervals=["1h", "4h"],
        lookback_hours=72,
    )
    assert config.symbol == "BTC-USDT-SWAP"
    assert config.lookback_hours == 72


def test_watch_config_creation() -> None:
    config = WatchConfig(
        symbol="XAU-USDT-SWAP",
        strategy_name="cm_macd_ult_mtf",
        interval="4h",
        method="ga",
        poll_seconds=120,
    )
    assert config.poll_seconds == 120
    assert config.interval == "4h"


def test_allowed_chat_matches_chat_id() -> None:
    bot = TelegramCommandBot(
        bot_token="bot",
        chat_id="12345",
        research_service=DummyResearchService(),
        monitor_once_callback=lambda **_: None,
    )

    class FakeMessage:
        chat_id = "12345"

    class FakeUpdate:
        message = FakeMessage()

    assert bot._allowed_chat(FakeUpdate()) is True

    class FakeUpdateWrong:
        message = type("M", (), {"chat_id": "99999"})()

    assert bot._allowed_chat(FakeUpdateWrong()) is False


def test_redis_key_includes_chat_id() -> None:
    bot = TelegramCommandBot(
        bot_token="bot",
        chat_id="12345",
        research_service=DummyResearchService(),
        monitor_once_callback=lambda **_: None,
    )
    assert bot._redis_key("rank:last") == "vntdr:rank:last:12345"
    assert bot._redis_key("watch") == "vntdr:watch:12345"


@pytest.mark.asyncio
async def test_execute_rank_sends_without_markdown_mode() -> None:
    """Ranking text may contain underscores (e.g. cm_macd_ult_mtf); sending
    with parse_mode="Markdown" triggers a BadRequest from Telegram.
    """
    research = MagicMock(spec=DummyResearchService)
    research.rank_intervals.return_value = [
        IntervalResearchResult(
            interval="15m",
            total_return=0.05,
            max_drawdown=0.01,
            sharpe_ratio=1.5,
            trade_count=10,
            best_parameters={"fast_length": 3},
            sync_inserted_count=300,
        ),
    ]
    research.format_rankings.return_value = "📊 XAU-USDT-SWAP cm_macd_ult_mtf 排名结果\n参数: {'fast_length': 3}"

    bot = TelegramCommandBot(
        bot_token="bot",
        chat_id="chat",
        research_service=research,
        monitor_once_callback=lambda **_: None,
    )

    reply_text_mock = AsyncMock()
    message_mock = MagicMock()
    message_mock.reply_text = reply_text_mock

    update_mock = MagicMock()
    update_mock.message = message_mock

    context_mock = MagicMock()
    context_mock.user_data = {}

    config = RankConfig(
        symbol="XAU-USDT-SWAP",
        strategy_name="cm_macd_ult_mtf",
        method="ga",
        intervals=["15m"],
        lookback_hours=24,
    )

    await bot._execute_rank(update_mock, context_mock, config, edit=False)

    assert reply_text_mock.call_count == 2  # loading message + result
    # Ensure Markdown mode is NOT used (second call is the ranking result)
    _, kwargs = reply_text_mock.call_args
    assert "parse_mode" not in kwargs


@pytest.mark.asyncio
async def test_execute_rank_edit_mode_sends_without_markdown_mode() -> None:
    research = MagicMock(spec=DummyResearchService)
    research.rank_intervals.return_value = [
        IntervalResearchResult(
            interval="4h",
            total_return=0.03,
            max_drawdown=0.02,
            sharpe_ratio=1.2,
            trade_count=5,
            best_parameters={"slow_length": 6},
            sync_inserted_count=200,
        ),
    ]
    research.format_rankings.return_value = "📊 BTC-USDT-SWAP cm_macd_ult_mtf 排名结果"

    bot = TelegramCommandBot(
        bot_token="bot",
        chat_id="chat",
        research_service=research,
        monitor_once_callback=lambda **_: None,
    )

    edit_mock = AsyncMock()
    message_mock = MagicMock()
    message_mock.reply_text = AsyncMock()

    query_mock = MagicMock()
    query_mock.edit_message_text = edit_mock
    query_mock.message = message_mock

    context_mock = MagicMock()
    context_mock.user_data = {}

    config = RankConfig(
        symbol="BTC-USDT-SWAP",
        strategy_name="cm_macd_ult_mtf",
        method="ga",
        intervals=["4h"],
        lookback_hours=48,
    )

    await bot._execute_rank(query_mock, context_mock, config, edit=True)

    # edit_message_text should be called for the final result
    assert edit_mock.call_count == 2  # loading message + result
    _, kwargs = edit_mock.call_args
    assert "parse_mode" not in kwargs
