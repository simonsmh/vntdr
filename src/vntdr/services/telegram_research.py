from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from vntdr.cleaning import INTERVAL_TO_DELTA
from vntdr.config import Settings
from vntdr.models import ResearchJobConfig
from vntdr.services.history import HistorySyncService
from vntdr.services.research import ResearchService


@dataclass
class IntervalResearchResult:
    interval: str
    total_return: float
    sharpe_ratio: float
    max_drawdown: float
    trade_count: float
    best_parameters: dict[str, Any]
    sync_inserted_count: int


class TelegramResearchService:
    DEFAULT_INTERVALS = ("15m", "30m", "1h", "4h")
    DEFAULT_METHODS = ("grid", "ga")
    DEFAULT_LOOKBACK_HOURS = 24

    def __init__(
        self,
        *,
        settings: Settings,
        history_service: HistorySyncService,
        research_service: ResearchService,
    ) -> None:
        self.settings = settings
        self.history_service = history_service
        self.research_service = research_service

    def available_intervals(self) -> list[str]:
        return [interval for interval in self.DEFAULT_INTERVALS if interval in INTERVAL_TO_DELTA]

    def available_methods(self) -> list[str]:
        return list(self.DEFAULT_METHODS)

    def default_symbol(self) -> str:
        return self.settings.research.default_symbol

    def default_strategy(self) -> str:
        return self.settings.research.default_strategy

    def default_method(self) -> str:
        return self.DEFAULT_METHODS[0]

    def default_lookback_hours(self) -> int:
        return self.DEFAULT_LOOKBACK_HOURS

    def default_ranking_intervals(self) -> list[str]:
        return self.available_intervals()

    def available_strategies(self) -> list[str]:
        strategies_dir = Path(__file__).resolve().parents[1] / "strategies"
        names = []
        for path in sorted(strategies_dir.glob("*.py")):
            if path.stem.startswith("_") or path.stem in {"__init__", "base"}:
                continue
            names.append(path.stem)
        return names

    def rank_intervals(
        self,
        *,
        symbol: str,
        strategy_name: str,
        method: str,
        intervals: list[str],
        lookback_hours: int = 24,
        fill_missing: bool = True,
    ) -> list[IntervalResearchResult]:
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=lookback_hours)
        parameter_space = self.research_service.default_parameter_space(strategy_name)
        results: list[IntervalResearchResult] = []

        for interval in intervals:
            sync = self.history_service.sync(
                symbol=symbol,
                interval=interval,
                start=start,
                end=end,
                fill_missing=fill_missing,
            )
            report = self.research_service.optimize(
                ResearchJobConfig(
                    strategy_name=strategy_name,
                    symbol=symbol,
                    interval=interval,
                    start=start,
                    end=end,
                    mode="optimize",
                    parameter_space=parameter_space,
                ),
                method=method,
            )
            results.append(
                IntervalResearchResult(
                    interval=interval,
                    total_return=float(report.metrics.get("total_return", 0.0)),
                    sharpe_ratio=float(report.metrics.get("sharpe_ratio", 0.0)),
                    max_drawdown=float(report.metrics.get("max_drawdown", 0.0)),
                    trade_count=float(report.metrics.get("trade_count", 0.0)),
                    best_parameters=report.best_parameters,
                    sync_inserted_count=sync.inserted_count,
                )
            )

        return sorted(results, key=lambda item: item.total_return, reverse=True)

    def format_rankings(
        self,
        *,
        symbol: str,
        strategy_name: str,
        method: str,
        lookback_hours: int,
        rankings: list[IntervalResearchResult],
    ) -> str:
        lines = [
            f"{symbol} {strategy_name} {lookback_hours}h ranking",
            f"method={method}",
        ]
        for index, item in enumerate(rankings, start=1):
            lines.append(
                f"{index}. {item.interval} return={item.total_return:.4f} "
                f"sharpe={item.sharpe_ratio:.4f} drawdown={item.max_drawdown:.4f} "
                f"trades={item.trade_count:.0f}"
            )
            lines.append(f"params={item.best_parameters}")
        return "\n".join(lines)
