from __future__ import annotations

from dataclasses import dataclass, field

from vntdr.config import Settings
from vntdr.services.monitoring import MonitoringService
from vntdr.services.research import ResearchService
from vntdr.services.risk import RiskManager
from vntdr.storage.database import Database
from vntdr.storage.repositories import MarketDataRepository, ResearchRunRepository


@dataclass
class FakeNotifier:
    messages: list[str] = field(default_factory=list)

    def notify(self, message: str) -> None:
        self.messages.append(message)


@dataclass
class FakeExecutor:
    actions: list[str] = field(default_factory=list)

    def execute(self, instructions):
        self.actions.extend([instruction.action for instruction in instructions])
        return instructions


class MemorySignalStore:
    def __init__(self, initial: dict[str, int] | None = None) -> None:
        self.values = initial or {}

    def get(self, key: str) -> int | None:
        return self.values.get(key)

    def set(self, key: str, value: int) -> None:
        self.values[key] = value


def test_monitoring_reverses_from_long_to_short_and_notifies(
    tmp_path,
    env_map: dict[str, str],
    sample_xau_bar_payloads: list[dict[str, object]],
) -> None:
    db_path = tmp_path / "research.sqlite3"
    database = Database(f"sqlite+pysqlite:///{db_path}")
    database.create_schema()
    market_repo = MarketDataRepository(database)
    market_repo.upsert_bars_from_payloads(sample_xau_bar_payloads)

    settings = Settings.from_mapping(
        {
            **env_map,
            "TG_BOT_TOKEN": "bot-token",
            "TG_CHAT_ID": "chat-id",
            "VNTDR_DATABASE_URL": f"sqlite+pysqlite:///{db_path}",
            "VNTDR_REPORT_DIR": str(tmp_path / "reports"),
        }
    )
    research_service = ResearchService(
        settings=settings,
        market_data_repository=market_repo,
        research_run_repository=ResearchRunRepository(database),
    )
    notifier = FakeNotifier()
    executor = FakeExecutor()
    state_store = MemorySignalStore({"signal:XAUUSDT:4h:cm_macd_ult_mtf": 1})
    service = MonitoringService(
        research_service=research_service,
        market_data_repository=market_repo,
        notifier=notifier,
        order_executor=executor,
        signal_store=state_store,
        risk_manager=RiskManager(settings.risk),
    )

    result = service.monitor_once(
        strategy_name="cm_macd_ult_mtf",
        symbol="XAUUSDT",
        interval="4h",
        parameter_space={
            "fast_length": [3, 4],
            "slow_length": [6, 7],
            "signal_length": [3],
            "trend_window": [2, 3],
        },
        volume=1.0,
    )

    assert result.signal == -1
    assert executor.actions == ["sell_long", "sell_short"]
    assert notifier.messages and "XAUUSDT" in notifier.messages[0]
    assert state_store.get("signal:XAUUSDT:4h:cm_macd_ult_mtf") == -1
