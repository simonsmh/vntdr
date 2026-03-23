from __future__ import annotations

import importlib
import itertools
import json
import random
from dataclasses import dataclass
from math import sqrt
from statistics import mean, pstdev
from typing import Any

from vntdr.config import Settings
from vntdr.models import BarRecord, FoldResult, ResearchJobConfig, ResearchReport, aggregate_metrics
from vntdr.storage.repositories import MarketDataRepository, ResearchRunRepository


@dataclass
class BacktestOutcome:
    metrics: dict[str, float]
    equity_curve: list[float]
    signals: list[int]


class ResearchService:
    def __init__(
        self,
        *,
        settings: Settings,
        market_data_repository: MarketDataRepository,
        research_run_repository: ResearchRunRepository,
    ) -> None:
        self.settings = settings
        self.market_data_repository = market_data_repository
        self.research_run_repository = research_run_repository
        self.settings.research.report_dir.mkdir(parents=True, exist_ok=True)

    def backtest(self, config: ResearchJobConfig) -> ResearchReport:
        bars = self._load_bars(config)
        report = self._build_report(config, bars, parameters=config.parameters)
        self._persist_report(report, config)
        return report

    def optimize(self, config: ResearchJobConfig, method: str = "grid") -> ResearchReport:
        bars = self._load_bars(config)
        evaluations = self._evaluate_parameter_space(
            bars=bars,
            strategy_name=config.strategy_name,
            parameter_space=config.parameter_space,
            method=method,
        )
        best_parameters, best_metrics = evaluations[0]
        report = ResearchReport(
            strategy_name=config.strategy_name,
            symbol=config.symbol,
            interval=config.interval,
            mode="optimize",
            metrics=best_metrics,
            best_parameters=best_parameters,
            top_results=[
                {
                    **parameters,
                    "score": metrics["sharpe_ratio"],
                    "total_return": metrics["total_return"],
                }
                for parameters, metrics in evaluations[:5]
            ],
        )
        self._persist_report(report, config.model_copy(update={"mode": "optimize"}))
        return report

    def walk_forward(self, config: ResearchJobConfig) -> ResearchReport:
        bars = self._load_bars(config)
        folds: list[FoldResult] = []
        metric_rows: list[dict[str, float]] = []
        offset = 0
        fold_index = 1
        run_stub = ResearchReport(
            strategy_name=config.strategy_name,
            symbol=config.symbol,
            interval=config.interval,
            mode="walk-forward",
            metrics={},
            best_parameters={},
        )
        run_id = self.research_run_repository.create_research_run(run_stub, config.model_dump(mode="json"))
        while offset + config.train_window + config.test_window <= len(bars):
            train_bars = bars[offset : offset + config.train_window]
            test_bars = bars[
                offset + config.train_window : offset + config.train_window + config.test_window
            ]
            evaluations = self._evaluate_parameter_space(
                bars=train_bars,
                strategy_name=config.strategy_name,
                parameter_space=config.parameter_space,
                method="grid",
            )
            best_parameters, _ = evaluations[0]
            outcome = self._execute_backtest(test_bars, config.strategy_name, best_parameters)
            fold = FoldResult(
                fold_index=fold_index,
                train_start=train_bars[0].datetime,
                train_end=train_bars[-1].datetime,
                test_start=test_bars[0].datetime,
                test_end=test_bars[-1].datetime,
                metrics=outcome.metrics,
                parameters=best_parameters,
            )
            folds.append(fold)
            metric_rows.append(outcome.metrics)
            self.research_run_repository.add_fold_result(run_id, fold)
            offset += config.test_window
            fold_index += 1

        aggregate = aggregate_metrics(metric_rows)
        best_parameters = folds[-1].parameters if folds else {}
        report = ResearchReport(
            strategy_name=config.strategy_name,
            symbol=config.symbol,
            interval=config.interval,
            mode="walk-forward",
            metrics=aggregate,
            best_parameters=best_parameters,
            fold_results=folds,
            top_results=[{"fold_count": len(folds), "score": aggregate.get("sharpe_ratio", 0.0)}],
        )
        self._persist_report(
            report,
            config.model_copy(update={"mode": "walk-forward"}),
            run_id=run_id,
        )
        return report

    def _build_report(
        self,
        config: ResearchJobConfig,
        bars: list[BarRecord],
        *,
        parameters: dict[str, Any],
    ) -> ResearchReport:
        outcome = self._execute_backtest(bars, config.strategy_name, parameters)
        return ResearchReport(
            strategy_name=config.strategy_name,
            symbol=config.symbol,
            interval=config.interval,
            mode=config.mode,
            metrics=outcome.metrics,
            best_parameters=parameters,
        )

    def _persist_report(
        self,
        report: ResearchReport,
        config: ResearchJobConfig,
        *,
        run_id: int | None = None,
    ) -> None:
        output_dir = self.settings.research.report_dir
        output_dir.mkdir(parents=True, exist_ok=True)
        slug = config.report_slug
        markdown_path = output_dir / f"{config.strategy_name}_{slug}.md"
        json_path = output_dir / f"{config.strategy_name}_{slug}.json"
        markdown_path.write_text(report.to_markdown(), encoding="utf-8")
        json_path.write_text(report.to_json(), encoding="utf-8")

        if run_id is None:
            run_id = self.research_run_repository.create_research_run(
                report,
                config.model_dump(mode="json"),
            )
        self.research_run_repository.finalize_research_run(
            run_id,
            status="completed",
            metrics=report.metrics,
            best_parameters=report.best_parameters,
            top_results=report.top_results,
            report_path=str(markdown_path),
        )

    def _load_bars(self, config: ResearchJobConfig) -> list[BarRecord]:
        bars = self.market_data_repository.fetch_bars(
            symbol=config.symbol,
            interval=config.interval,
            start=config.start,
            end=config.end,
        )
        if not bars:
            raise ValueError("No bars found for the requested research job.")
        return bars

    def default_parameters(self, strategy_name: str) -> dict[str, Any]:
        strategy = self._load_strategy(strategy_name)
        return dict(getattr(strategy, "DEFAULT_PARAMETERS", getattr(strategy, "defaults", {})))

    def default_parameter_space(self, strategy_name: str) -> dict[str, list[Any]]:
        strategy = self._load_strategy(strategy_name)
        return dict(getattr(strategy, "DEFAULT_PARAMETER_SPACE", {}))

    def optimize_parameters(
        self,
        *,
        strategy_name: str,
        bars: list[BarRecord],
        parameter_space: dict[str, list[Any]],
        method: str = "grid",
    ) -> tuple[dict[str, Any], dict[str, float], list[tuple[dict[str, Any], dict[str, float]]]]:
        evaluations = self._evaluate_parameter_space(
            bars=bars,
            strategy_name=strategy_name,
            parameter_space=parameter_space,
            method=method,
        )
        best_parameters, best_metrics = evaluations[0]
        return best_parameters, best_metrics, evaluations

    def latest_signal(
        self,
        *,
        strategy_name: str,
        bars: list[BarRecord],
        parameters: dict[str, Any],
    ) -> int:
        strategy = self._load_strategy(strategy_name)
        if not bars:
            return 0
        return int(strategy.signal_for_index(bars, len(bars) - 1, parameters))

    def _evaluate_parameter_space(
        self,
        *,
        bars: list[BarRecord],
        strategy_name: str,
        parameter_space: dict[str, list[Any]],
        method: str,
    ) -> list[tuple[dict[str, Any], dict[str, float]]]:
        return (
            self._run_genetic_search(bars, strategy_name, parameter_space)
            if method == "ga"
            else self._run_grid_search_on_bars(bars, strategy_name, parameter_space)
        )

    def _run_grid_search(
        self,
        bars: list[BarRecord],
        config: ResearchJobConfig,
    ) -> list[tuple[dict[str, Any], dict[str, float]]]:
        return self._run_grid_search_on_bars(bars, config.strategy_name, config.parameter_space)

    def _run_grid_search_on_bars(
        self,
        bars: list[BarRecord],
        strategy_name: str,
        parameter_space: dict[str, list[Any]],
    ) -> list[tuple[dict[str, Any], dict[str, float]]]:
        keys = list(parameter_space.keys())
        combos = itertools.product(*(parameter_space[key] for key in keys))
        evaluations = []
        for combo in combos:
            parameters = dict(zip(keys, combo, strict=True))
            outcome = self._execute_backtest(bars, strategy_name, parameters)
            evaluations.append((parameters, outcome.metrics))
        return sorted(evaluations, key=lambda item: item[1]["sharpe_ratio"], reverse=True)

    def _run_genetic_search(
        self,
        bars: list[BarRecord],
        strategy_name: str,
        parameter_space: dict[str, list[Any]],
    ) -> list[tuple[dict[str, Any], dict[str, float]]]:
        keys = list(parameter_space.keys())
        population = [
            {key: random.choice(parameter_space[key]) for key in keys}
            for _ in range(max(4, len(keys) * 2))
        ]
        evaluations: dict[str, tuple[dict[str, Any], dict[str, float]]] = {}
        for _ in range(5):
            scored = []
            for parameters in population:
                signature = json.dumps(parameters, sort_keys=True)
                if signature not in evaluations:
                    outcome = self._execute_backtest(bars, strategy_name, parameters)
                    evaluations[signature] = (parameters.copy(), outcome.metrics)
                scored.append(evaluations[signature])
            scored.sort(key=lambda item: item[1]["sharpe_ratio"], reverse=True)
            parents = [parameters for parameters, _ in scored[:2]]
            next_population = parents.copy()
            while len(next_population) < len(population):
                parent_a = random.choice(parents)
                parent_b = random.choice(parents)
                child = {
                    key: random.choice([parent_a[key], parent_b[key], random.choice(parameter_space[key])])
                    for key in keys
                }
                next_population.append(child)
            population = next_population
        return sorted(evaluations.values(), key=lambda item: item[1]["sharpe_ratio"], reverse=True)

    def _execute_backtest(
        self,
        bars: list[BarRecord],
        strategy_name: str,
        parameters: dict[str, Any],
    ) -> BacktestOutcome:
        strategy = self._load_strategy(strategy_name)
        position = 0
        trade_count = 0
        equity = [1.0]
        step_returns: list[float] = []
        signals: list[int] = []
        for index in range(1, len(bars)):
            signal = strategy.signal_for_index(bars, index, parameters)
            signals.append(signal)
            if signal != position:
                trade_count += 1
                position = signal
            price_return = (bars[index].close / bars[index - 1].close) - 1
            pnl = price_return * position
            step_returns.append(pnl)
            equity.append(equity[-1] * (1 + pnl))

        metrics = self._metrics_from_returns(step_returns, equity, trade_count)
        return BacktestOutcome(metrics=metrics, equity_curve=equity, signals=signals)

    def _load_strategy(self, strategy_name: str) -> Any:
        module = importlib.import_module(f"vntdr.strategies.{strategy_name}")
        strategy = getattr(module, "Strategy", None)
        if strategy is None:
            raise ImportError(f"Strategy module vntdr.strategies.{strategy_name} has no Strategy class.")
        defaults = getattr(module, "DEFAULT_PARAMETERS", None)
        parameter_space = getattr(module, "DEFAULT_PARAMETER_SPACE", None)
        if defaults is not None:
            setattr(strategy, "DEFAULT_PARAMETERS", defaults)
        if parameter_space is not None:
            setattr(strategy, "DEFAULT_PARAMETER_SPACE", parameter_space)
        return strategy

    def _metrics_from_returns(
        self,
        returns: list[float],
        equity_curve: list[float],
        trade_count: int,
    ) -> dict[str, float]:
        if not returns:
            return {
                "total_return": 0.0,
                "sharpe_ratio": 0.0,
                "max_drawdown": 0.0,
                "trade_count": float(trade_count),
            }
        avg_return = mean(returns)
        volatility = pstdev(returns)
        sharpe = (avg_return / volatility * (len(returns) ** 0.5)) if volatility else 0.0
        peak = equity_curve[0]
        max_drawdown = 0.0
        for value in equity_curve:
            peak = max(peak, value)
            max_drawdown = min(max_drawdown, (value / peak) - 1)
        return {
            "total_return": equity_curve[-1] - 1,
            "sharpe_ratio": sharpe,
            "max_drawdown": max_drawdown,
            "trade_count": float(trade_count),
        }
