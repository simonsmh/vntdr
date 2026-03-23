from __future__ import annotations

from vntdr.config import Settings
from vntdr.models import ResearchJobConfig, ResearchReport, SyncResult
from vntdr.services.telegram_research import TelegramResearchService


class StubHistoryService:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def sync(self, **kwargs) -> SyncResult:
        self.calls.append(kwargs)
        return SyncResult(job_id=1, inserted_count=5, cleaned_count=5, duplicates_removed=0)


class StubResearchService:
    def __init__(self) -> None:
        self.calls: list[tuple[ResearchJobConfig, str]] = []

    def default_parameter_space(self, strategy_name: str) -> dict[str, list[int]]:
        assert strategy_name == "cm_macd_ult_mtf"
        return {"fast_length": [3, 4], "slow_length": [6, 7]}

    def optimize(self, config: ResearchJobConfig, method: str = "grid") -> ResearchReport:
        self.calls.append((config, method))
        total_return = {"15m": 0.12, "30m": 0.08, "1h": 0.15}[config.interval]
        return ResearchReport(
            strategy_name=config.strategy_name,
            symbol=config.symbol,
            interval=config.interval,
            mode="optimize",
            metrics={
                "total_return": total_return,
                "sharpe_ratio": total_return * 10,
                "max_drawdown": -0.03,
                "trade_count": 4.0,
            },
            best_parameters={"fast_length": 3, "slow_length": 6},
        )


def test_rank_intervals_sorts_by_total_return(env_map: dict[str, str]) -> None:
    service = TelegramResearchService(
        settings=Settings.from_mapping(env_map),
        history_service=StubHistoryService(),
        research_service=StubResearchService(),
    )

    rankings = service.rank_intervals(
        symbol="XAUUSDT",
        strategy_name="cm_macd_ult_mtf",
        method="grid",
        intervals=["15m", "30m", "1h"],
        lookback_hours=24,
    )

    assert [item.interval for item in rankings] == ["1h", "15m", "30m"]
    assert all(item.best_parameters["fast_length"] == 3 for item in rankings)


def test_format_rankings_includes_symbol_and_parameters(env_map: dict[str, str]) -> None:
    service = TelegramResearchService(
        settings=Settings.from_mapping(env_map),
        history_service=StubHistoryService(),
        research_service=StubResearchService(),
    )

    message = service.format_rankings(
        symbol="XAUUSDT",
        strategy_name="cm_macd_ult_mtf",
        method="grid",
        lookback_hours=24,
        rankings=service.rank_intervals(
            symbol="XAUUSDT",
            strategy_name="cm_macd_ult_mtf",
            method="grid",
            intervals=["15m"],
            lookback_hours=24,
        ),
    )

    assert "XAUUSDT cm_macd_ult_mtf 24h ranking" in message
    assert "params={'fast_length': 3, 'slow_length': 6}" in message
