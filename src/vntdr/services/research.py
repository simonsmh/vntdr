from __future__ import annotations

import importlib
import itertools
import json
import random
from dataclasses import dataclass
import math
from statistics import mean, pstdev, stdev
from typing import Any
import asyncio
from concurrent.futures import ThreadPoolExecutor

from vntdr.config import Settings
from vntdr.models import BarRecord, FoldResult, ResearchJobConfig, ResearchReport, aggregate_metrics
from vntdr.storage.repositories import MarketDataRepository, ResearchRunRepository


@dataclass
class BacktestOutcome:
    metrics: dict[str, float]
    equity_curve: list[float]
    signals: list[int]


@dataclass
class BacktestResult:
    outcome: BacktestOutcome
    bars: list[BarRecord]
    parameters: dict[str, Any]


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
        self._executor = ThreadPoolExecutor(max_workers=4)

    def backtest(self, config: ResearchJobConfig) -> ResearchReport:
        bars = self._load_bars(config)
        report = self._build_report(config, bars, parameters=config.parameters)
        self._persist_report(report, config)
        return report

    def backtest_with_details(self, config: ResearchJobConfig) -> BacktestResult:
        bars = self._load_bars(config)
        outcome = self._execute_backtest(bars, config.strategy_name, config.parameters)
        return BacktestResult(outcome=outcome, bars=bars, parameters=config.parameters)

    async def backtest_async(self, config: ResearchJobConfig) -> ResearchReport:
        return await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self.backtest,
            config
        )

    def optimize(self, config: ResearchJobConfig, method: str = "ga") -> ResearchReport:
        bars = self._load_bars(config)
        evaluations = self._evaluate_parameter_space(
            bars=bars,
            strategy_name=config.strategy_name,
            parameter_space=config.parameter_space,
            method=method,
            optimize_target=config.optimize_target,
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
                    "score": metrics["total_return"] if config.optimize_target == "return" else metrics["sharpe_ratio"],
                    "sharpe_ratio": metrics["sharpe_ratio"],
                    "total_return": metrics["total_return"],
                }
                for parameters, metrics in evaluations[:5]
            ],
        )
        self._persist_report(report, config.model_copy(update={"mode": "optimize"}))
        return report

    async def optimize_async(self, config: ResearchJobConfig, method: str = "ga") -> ResearchReport:
        return await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self.optimize,
            config,
            method
        )

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
                method=config.method,
                optimize_target=config.optimize_target,
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

    async def walk_forward_async(self, config: ResearchJobConfig) -> ResearchReport:
        return await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self.walk_forward,
            config
        )

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
        overrides = getattr(self.settings.research, "strategy_parameters", {})
        if overrides and strategy_name in overrides:
            return dict(overrides[strategy_name])
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
        method: str = "ga",
        optimize_target: str | None = None,
    ) -> tuple[dict[str, Any], dict[str, float], list[tuple[dict[str, Any], dict[str, float]]]]:
        if optimize_target is None:
            optimize_target = getattr(self.settings.research, "optimize_target", "sharpe")
        evaluations = self._evaluate_parameter_space(
            bars=bars,
            strategy_name=strategy_name,
            parameter_space=parameter_space,
            method=method,
            optimize_target=optimize_target,
        )
        best_parameters, best_metrics = evaluations[0]
        return best_parameters, best_metrics, evaluations

    async def optimize_parameters_async(
        self,
        *,
        strategy_name: str,
        bars: list[BarRecord],
        parameter_space: dict[str, list[Any]],
        method: str = "ga",
        optimize_target: str | None = None,
    ) -> tuple[dict[str, Any], dict[str, float], list[tuple[dict[str, Any], dict[str, float]]]]:
        return await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self.optimize_parameters,
            strategy_name,
            bars,
            parameter_space,
            method,
            optimize_target
        )

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
        sig = int(strategy.signal_for_index(bars, len(bars) - 1, parameters))
        trade_mode = getattr(self.settings.research, "trade_mode", "both")
        if trade_mode == "long_only" and sig < 0:
            return 0
        if trade_mode == "short_only" and sig > 0:
            return 0
        return sig

    async def latest_signal_async(
        self,
        *,
        strategy_name: str,
        bars: list[BarRecord],
        parameters: dict[str, Any],
    ) -> int:
        return await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self.latest_signal,
            strategy_name,
            bars,
            parameters
        )

    def _evaluate_parameter_space(
        self,
        *,
        bars: list[BarRecord],
        strategy_name: str,
        parameter_space: dict[str, list[Any]],
        method: str = "ga",
        optimize_target: str = "sharpe",
    ) -> list[tuple[dict[str, Any], dict[str, float]]]:
        m = str(method).lower().strip()
        
        # Calculate total combinations
        total_combinations = math.prod(len(v) for v in parameter_space.values()) if parameter_space else 0
        if total_combinations <= 1000:
            m = "grid"

        if m == "grid":
            return self._run_grid_search(bars, strategy_name, parameter_space, optimize_target)
        elif m in ("heuristic", "bfs", "astar"):
            return self._run_heuristic_search(bars, strategy_name, parameter_space, optimize_target)
        else:
            return self._run_genetic_search(bars, strategy_name, parameter_space, optimize_target)

    def _run_grid_search(
        self,
        bars: list[BarRecord],
        strategy_name: str,
        parameter_space: dict[str, list[Any]],
        optimize_target: str = "sharpe",
    ) -> list[tuple[dict[str, Any], dict[str, float]]]:
        keys = list(parameter_space.keys())
        value_lists = [parameter_space[k] for k in keys]
        combinations = list(itertools.product(*value_lists))
        
        evaluations = []
        for combo in combinations:
            params = dict(zip(keys, combo, strict=True))
            outcome = self._execute_backtest(bars, strategy_name, params)
            evaluations.append((params, outcome.metrics))
            
        return sorted(
            evaluations,
            key=lambda item: (
                -999.0 if item[1].get("trade_count", 0) == 0 else item[1].get("total_return", 0.0),
                -999.0 if item[1].get("trade_count", 0) == 0 else item[1].get("sharpe_ratio", 0.0)
            )
            if optimize_target == "return"
            else (
                -999.0 if item[1].get("trade_count", 0) == 0 else item[1].get("sharpe_ratio", 0.0),
                -999.0 if item[1].get("trade_count", 0) == 0 else item[1].get("total_return", 0.0)
            ),
            reverse=True
        )

    def _run_heuristic_search(
        self,
        bars: list[BarRecord],
        strategy_name: str,
        parameter_space: dict[str, list[Any]],
        optimize_target: str = "sharpe",
        max_evaluations: int = 100,
    ) -> list[tuple[dict[str, Any], dict[str, float]]]:
        """A*-inspired Heuristic graph search over parameter grid."""
        import heapq
        
        local_random = random.Random(42)
        keys = list(parameter_space.keys())
        dim_lengths = [len(parameter_space[k]) for k in keys]
        
        def node_to_params(node: tuple[int, ...]) -> dict[str, Any]:
            return {keys[i]: parameter_space[keys[i]][node[i]] for i in range(len(keys))}
            
        evaluations: dict[tuple[int, ...], tuple[dict[str, Any], dict[str, float]]] = {}
        
        def evaluate_node(node: tuple[int, ...]) -> float:
            if node in evaluations:
                _, metrics = evaluations[node]
            else:
                params = node_to_params(node)
                outcome = self._execute_backtest(bars, strategy_name, params)
                metrics = outcome.metrics
                evaluations[node] = (params, metrics)
                
            if metrics.get("trade_count", 0) == 0:
                return -999.0
            if optimize_target == "return":
                return metrics.get("total_return", 0.0)
            else:
                return metrics.get("sharpe_ratio", 0.0)

        # Seeds: Center of parameter grid + a few random points
        center_node = tuple(length // 2 for length in dim_lengths)
        seeds = {center_node}
        
        # Add up to 3 random seeds to avoid getting stuck in local optima
        num_random_seeds = min(3, math.prod(dim_lengths) - 1)
        while len(seeds) < num_random_seeds + 1:
            rand_node = tuple(local_random.randint(0, length - 1) for length in dim_lengths)
            seeds.add(rand_node)
            
        open_set = []
        visited = set()
        
        for seed in seeds:
            score = evaluate_node(seed)
            heapq.heappush(open_set, (-score, seed))
            visited.add(seed)
            
        eval_count = len(seeds)
        
        while open_set and eval_count < max_evaluations:
            neg_score, current = heapq.heappop(open_set)
            
            # Generate neighbors (step +/-1 in each dimension)
            neighbors = []
            for dim in range(len(keys)):
                for delta in (-1, 1):
                    neighbor = list(current)
                    neighbor[dim] += delta
                    if 0 <= neighbor[dim] < dim_lengths[dim]:
                        neighbors.append(tuple(neighbor))
                        
            for neighbor in neighbors:
                if neighbor not in visited:
                    visited.add(neighbor)
                    score = evaluate_node(neighbor)
                    eval_count += 1
                    
                    # Push with priority as -score
                    heapq.heappush(open_set, (-score, neighbor))
                    
                    if eval_count >= max_evaluations:
                        break
                        
        return sorted(
            evaluations.values(),
            key=lambda item: (
                -999.0 if item[1].get("trade_count", 0) == 0 else item[1].get("total_return", 0.0),
                -999.0 if item[1].get("trade_count", 0) == 0 else item[1].get("sharpe_ratio", 0.0)
            )
            if optimize_target == "return"
            else (
                -999.0 if item[1].get("trade_count", 0) == 0 else item[1].get("sharpe_ratio", 0.0),
                -999.0 if item[1].get("trade_count", 0) == 0 else item[1].get("total_return", 0.0)
            ),
            reverse=True
        )

    def _run_genetic_search(
        self,
        bars: list[BarRecord],
        strategy_name: str,
        parameter_space: dict[str, list[Any]],
        optimize_target: str = "sharpe",
    ) -> list[tuple[dict[str, Any], dict[str, float]]]:
        # Use a local Random instance with a fixed seed for 100% reproducibility
        local_random = random.Random(42)
        keys = list(parameter_space.keys())
        pop_size = max(20, len(keys) * 10)
        generations = 15

        population = [
            {key: local_random.choice(parameter_space[key]) for key in keys}
            for _ in range(pop_size)
        ]
        evaluations: dict[str, tuple[dict[str, Any], dict[str, float]]] = {}
        for _ in range(generations):
            scored = []
            for parameters in population:
                signature = json.dumps(parameters, sort_keys=True)
                if signature not in evaluations:
                    outcome = self._execute_backtest(bars, strategy_name, parameters)
                    evaluations[signature] = (parameters.copy(), outcome.metrics)
                scored.append(evaluations[signature])
            # Sort by target primarily, penalizing zero trades to avoid passive dominance
            if optimize_target == "return":
                key_fn = lambda item: (
                    -999.0 if item[1].get("trade_count", 0) == 0 else item[1].get("total_return", 0.0),
                    -999.0 if item[1].get("trade_count", 0) == 0 else item[1].get("sharpe_ratio", 0.0)
                )
            else:
                key_fn = lambda item: (
                    -999.0 if item[1].get("trade_count", 0) == 0 else item[1].get("sharpe_ratio", 0.0),
                    -999.0 if item[1].get("trade_count", 0) == 0 else item[1].get("total_return", 0.0)
                )
            scored.sort(key=key_fn, reverse=True)
            # Maintain top 20% as potential parents
            parents = [parameters for parameters, _ in scored[:max(2, pop_size // 5)]]
            # Elitism: keep top 2 directly
            next_population = [p.copy() for p, _ in scored[:2]]
            while len(next_population) < pop_size:
                parent_a = local_random.choice(parents)
                parent_b = local_random.choice(parents)
                child = {
                    key: local_random.choice([parent_a[key], parent_b[key], local_random.choice(parameter_space[key])])
                    for key in keys
                }
                next_population.append(child)
            population = next_population
        return sorted(
            evaluations.values(),
            key=lambda item: (
                -999.0 if item[1].get("trade_count", 0) == 0 else item[1].get("total_return", 0.0),
                -999.0 if item[1].get("trade_count", 0) == 0 else item[1].get("sharpe_ratio", 0.0)
            )
            if optimize_target == "return"
            else (
                -999.0 if item[1].get("trade_count", 0) == 0 else item[1].get("sharpe_ratio", 0.0),
                -999.0 if item[1].get("trade_count", 0) == 0 else item[1].get("total_return", 0.0)
            ),
            reverse=True
        )

    def _execute_backtest(
        self,
        bars: list[BarRecord],
        strategy_name: str,
        parameters: dict[str, Any],
    ) -> BacktestOutcome:
        if not bars:
            return BacktestOutcome(metrics={}, equity_curve=[], signals=[])

        strategy = self._load_strategy(strategy_name)
        position = 0
        trade_count = 0
        equity = [1.0]
        signals: list[int] = []
        
        # Get fee rate from settings
        fee_rate = (
            self.settings.research.maker_fee_rate
            if self.settings.research.use_maker_fee
            else self.settings.research.taker_fee_rate
        )
        
        # Backtest loop: 
        # 1. Calculate signal at end of bar 'index' (using data up to 'index')
        # 2. Execute trade at close price of bar 'index' (paying fees)
        # 3. Earn/lose return from bar 'index' to bar 'index + 1'
        for index in range(len(bars) - 1):
            signal = int(strategy.signal_for_index(bars, index, parameters))
            
            # Apply trade mode filtering
            trade_mode = getattr(self.settings.research, "trade_mode", "both")
            if trade_mode == "long_only" and signal < 0:
                signal = 0
            elif trade_mode == "short_only" and signal > 0:
                signal = 0
                
            signals.append(signal)
            
            if signal != position:
                if position != 0:
                    equity[-1] *= (1 - fee_rate)
                    trade_count += 1
                
                if signal != 0:
                    equity[-1] *= (1 - fee_rate)
                    trade_count += 1

                position = signal

            # PnL is realized from bar 'index' to 'index + 1'
            price_return = (bars[index + 1].close / bars[index].close) - 1
            pnl = price_return * position
            equity.append(equity[-1] * (1 + pnl))
            
        # Close final position if any to account for exit fees
        if position != 0:
            equity[-1] *= (1 - fee_rate)
            trade_count += 1

        # Calculate step returns from equity curve changes to include transaction fees
        step_returns = []
        for i in range(len(equity) - 1):
            if equity[i] > 0:
                step_returns.append((equity[i + 1] / equity[i]) - 1)
            else:
                step_returns.append(0.0)

        interval = bars[0].interval
        metrics = self._metrics_from_returns(step_returns, equity, trade_count, interval)
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
        interval: str = "1h",
    ) -> dict[str, float]:
        from vntdr.services.metrics import calculate_metrics
        return calculate_metrics(returns, equity_curve, trade_count, interval)
