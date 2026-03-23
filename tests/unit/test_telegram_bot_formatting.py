from __future__ import annotations

from vntdr.adapters.telegram_bot import RankConfig, TelegramCommandBot, WatchConfig
from vntdr.models import MonitorResult


class DummyResearchService:
    def default_symbol(self) -> str:
        return "XAUUSDT"

    def default_strategy(self) -> str:
        return "cm_macd_ult_mtf"

    def default_method(self) -> str:
        return "grid"

    def default_ranking_intervals(self) -> list[str]:
        return ["15m", "30m", "1h", "4h"]

    def default_lookback_hours(self) -> int:
        return 24


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

    assert "monitor XAUUSDT 15m" in message
    assert "actions=buy_short, buy_long" in message
    assert "parameters={'fast_length': 3}" in message


def test_format_watch_status_includes_poll_seconds() -> None:
    bot = TelegramCommandBot(
        bot_token="bot",
        chat_id="chat",
        research_service=DummyResearchService(),
        monitor_once_callback=lambda **_: None,
    )

    message = bot._format_watch_status(
        WatchConfig(
            symbol="XAUUSDT",
            strategy_name="cm_macd_ult_mtf",
            interval="15m",
            method="grid",
            poll_seconds=60,
        )
    )

    assert "watching XAUUSDT cm_macd_ult_mtf 15m" in message
    assert "every=60s" in message


def test_resolve_watch_top_rank_config_prefers_existing_context() -> None:
    bot = TelegramCommandBot(
        bot_token="bot",
        chat_id="chat",
        research_service=DummyResearchService(),
        monitor_once_callback=lambda **_: None,
    )

    class Context:
        user_data = {
            "symbol": "XAUUSDT",
            "strategy_name": "cm_macd_ult_mtf",
            "method": "ga",
            "intervals": ["15m", "1h"],
            "lookback_hours": 12,
            "watch_top_symbol": "BTC-USDT-SWAP",
        }

    config = bot._resolve_watch_top_rank_config(Context())

    assert config == RankConfig(
        symbol="BTC-USDT-SWAP",
        strategy_name="cm_macd_ult_mtf",
        method="ga",
        intervals=["15m", "1h"],
        lookback_hours=12,
    )
