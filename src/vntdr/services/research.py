from __future__ import annotations

import asyncio
import importlib
import itertools
import json
import math
import random
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

from vntdr.config import Settings
from vntdr.models import AblationResult, BarRecord, FoldResult, Instrument, Interval, ResearchJobConfig, ResearchReport, ResearchValidationResult, TradeRecord
from vntdr.services.data_context import MarketDataContext
from vntdr.services.position_sizing import AtrRiskSizer
from vntdr.storage.repositories import MarketDataRepository, ResearchRunRepository
from vntdr.storage.repositories import StrategyRepository

EXACT_SEARCH_COMBINATION_LIMIT = 10_000
VALID_TRADE_MODES = frozenset({"both", "long_only", "short_only"})


@dataclass
class BacktestOutcome:
    metrics: dict[str, float]
    equity_curve: list[float]
    signals: list[int]
    trades: list[TradeRecord] | None = None


@dataclass
class BacktestResult:
    outcome: BacktestOutcome
    bars: list[BarRecord]
    parameters: dict[str, Any]


@dataclass(frozen=True)
class CostModel:
    """Per-side costs applied at executable prices and while a position is held."""

    fee_rate: float
    slippage_bps: float = 0.0
    spread_bps: float = 0.0
    funding_rate_per_bar: float = 0.0

    @property
    def execution_cost_rate(self) -> float:
        return self.fee_rate + (self.slippage_bps + self.spread_bps / 2) / 10_000

    def fill_price(self, raw_price: float, side: int) -> float:
        """side=1 is buy (adverse upward), side=-1 is sell (adverse downward)."""
        return raw_price * (1 + side * (self.slippage_bps + self.spread_bps / 2) / 10_000)


class ResearchService:
    def __init__(
        self,
        *,
        settings: Settings,
        market_data_repository: MarketDataRepository,
        research_run_repository: ResearchRunRepository,
        factor_repository: StrategyRepository | None = None,
    ) -> None:
        self.settings = settings
        self.market_data_repository = market_data_repository
        self.research_run_repository = research_run_repository
        self.factor_repository = factor_repository
        self.settings.research.report_dir.mkdir(parents=True, exist_ok=True)
        self._executor = ThreadPoolExecutor(max_workers=4)

    def backtest(self, config: ResearchJobConfig) -> ResearchReport:
        bars = self._load_bars(config)
        context = self._load_data_context(config, bars)
        report = self._build_report(config, bars, parameters=config.parameters, data_context=context)
        self._persist_report(report, config)
        return report

    def backtest_with_details(self, config: ResearchJobConfig) -> BacktestResult:
        bars = self._load_bars(config)
        outcome = self._execute_backtest(
            bars, config.strategy_name, config.parameters,
            data_context=self._load_data_context(config, bars),
        )
        return BacktestResult(outcome=outcome, bars=bars, parameters=config.parameters)

    def factor_ablation(
        self,
        config: ResearchJobConfig,
        variants: dict[str, dict[str, Any]],
    ) -> AblationResult:
        """Evaluate named parameter overrides against exactly the same bars.

        Variants are deliberately supplied explicitly instead of optimized, so
        an ablation cannot smuggle in different fitted parameters.
        """
        bars = self._load_bars(config)
        context = self._load_data_context(config, bars)
        rows = []
        for name, overrides in variants.items():
            parameters = {**config.parameters, **overrides}
            outcome = self._execute_backtest(
                bars, config.strategy_name, parameters, data_context=context
            )
            rows.append({"name": name, "parameters": parameters, "metrics": outcome.metrics})
        return AblationResult(
            strategy_name=config.strategy_name, symbol=config.symbol,
            interval=config.interval, variants=rows,
        )

    def validate_candidate(
        self,
        *,
        backtest_config: ResearchJobConfig,
        walk_forward_config: ResearchJobConfig,
        max_drawdown_limit: float = 0.10,
        minimum_fold_count: int = 3,
        minimum_trade_count: int = 10,
    ) -> ResearchValidationResult:
        """Run reproducible in-sample and out-of-sample acceptance gates.

        The validation does not optimise the backtest configuration. The
        walk-forward configuration alone selects each fold's parameters from
        historical data, preserving the approval boundary.
        """
        if backtest_config.mode != "backtest":
            raise ValueError("backtest_config.mode must be 'backtest'")
        if walk_forward_config.mode != "walk-forward":
            raise ValueError("walk_forward_config.mode must be 'walk-forward'")
        identity = ("strategy_name", "symbol", "exchange", "interval")
        if any(getattr(backtest_config, name) != getattr(walk_forward_config, name) for name in identity):
            raise ValueError("backtest and walk-forward configurations must reference the same dataset")
        backtest = self.backtest(backtest_config)
        walk_forward = self.walk_forward(walk_forward_config)
        reasons: list[str] = []
        if backtest.metrics.get("trade_count", 0) < minimum_trade_count:
            reasons.append(f"backtest_trade_count<{minimum_trade_count}")
        if len(walk_forward.fold_results) < minimum_fold_count:
            reasons.append(f"walk_forward_fold_count<{minimum_fold_count}")
        maximum_drawdown = walk_forward.metrics.get("max_drawdown", 0.0)
        if maximum_drawdown < -abs(max_drawdown_limit):
            reasons.append(f"walk_forward_drawdown>{abs(max_drawdown_limit):.2%}")
        if walk_forward.metrics.get("total_return", 0.0) <= 0:
            reasons.append("walk_forward_total_return<=0")
        return ResearchValidationResult(
            backtest=backtest,
            walk_forward=walk_forward,
            passed=not reasons,
            reasons=reasons,
            max_drawdown_limit=max_drawdown_limit,
            minimum_fold_count=minimum_fold_count,
        )

    async def backtest_async(self, config: ResearchJobConfig) -> ResearchReport:
        return await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self.backtest,
            config
        )

    def optimize(self, config: ResearchJobConfig, method: str = "ga") -> ResearchReport:
        bars = self._load_bars(config)
        context = self._load_data_context(config, bars)
        evaluations = self._evaluate_parameter_space(
            bars=bars,
            strategy_name=config.strategy_name,
            parameter_space=config.parameter_space,
            base_parameters=config.parameters,
            method=method,
            optimize_target=config.optimize_target,
            data_context=context,
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
        context = self._load_data_context(config, bars)
        folds: list[FoldResult] = []
        out_of_sample_returns: list[float] = []
        stitched_equity: list[float] = [1.0]
        out_of_sample_trades: list[TradeRecord] = []
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
                base_parameters=config.parameters,
                method=config.method,
                optimize_target=config.optimize_target,
                data_context=context,
            )
            best_parameters, _ = evaluations[0]
            # Preserve the complete training history as indicator warm-up. The
            # final closed training bar makes the first out-of-sample decision,
            # which is filled at the first test-bar open. Only test-period
            # transitions contribute to performance.
            evaluation_bars = train_bars + test_bars
            outcome = self._execute_backtest(
                evaluation_bars,
                config.strategy_name,
                best_parameters,
                decision_start_index=len(train_bars) - 1,
                data_context=context,
            )
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
            fold_returns = [
                outcome.equity_curve[i] / outcome.equity_curve[i - 1] - 1
                for i in range(1, len(outcome.equity_curve))
                if outcome.equity_curve[i - 1] > 0
            ]
            for fold_return in fold_returns:
                out_of_sample_returns.append(fold_return)
                stitched_equity.append(stitched_equity[-1] * (1 + fold_return))
            out_of_sample_trades.extend(outcome.trades or [])
            self.research_run_repository.add_fold_result(run_id, fold)
            offset += config.test_window
            fold_index += 1

        aggregate = self._metrics_from_returns(
            out_of_sample_returns,
            stitched_equity,
            len(out_of_sample_trades),
            bars[0].interval,
        )
        aggregate.update(self._trade_metrics(out_of_sample_trades))
        aggregate["turnover"] = round(
            sum(abs(trade.gross_return) for trade in out_of_sample_trades),
            6,
        )
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
        data_context: MarketDataContext | None = None,
    ) -> ResearchReport:
        outcome = self._execute_backtest(
            bars, config.strategy_name, parameters, data_context=data_context
        )
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
            exchange=config.exchange,
        )
        if not bars:
            raise ValueError("No bars found for the requested research job.")
        return bars

    def _load_data_context(
        self, config: ResearchJobConfig, primary_bars: list[BarRecord]
    ) -> MarketDataContext | None:
        if not config.auxiliary_intervals and self.factor_repository is None:
            return None
        bars_by_interval = {Interval(value=config.interval).value: primary_bars}
        for interval in config.auxiliary_intervals:
            bars_by_interval[interval.value] = self.market_data_repository.fetch_bars(
                symbol=config.symbol,
                interval=interval.value,
                start=config.start,
                end=config.end,
                exchange=config.exchange,
            )
        factors = []
        if self.factor_repository is not None:
            exchange = config.exchange or primary_bars[0].exchange
            factors = self.factor_repository.factors_available_at(
                Instrument(symbol=config.symbol, exchange=exchange), config.end
            )
        return MarketDataContext(bars_by_interval, factors=factors)

    def default_parameters(self, strategy_name: str) -> dict[str, Any]:
        strategy = self._load_strategy(strategy_name)
        defaults = dict(getattr(strategy, "DEFAULT_PARAMETERS", getattr(strategy, "defaults", {})))
        overrides = getattr(self.settings.research, "strategy_parameters", {})
        if overrides and strategy_name in overrides:
            # Persisted UI/Telegram overrides often predate newly introduced
            # safe defaults. Merge rather than replacing the strategy schema.
            return {**defaults, **dict(overrides[strategy_name])}
        return defaults

    def default_parameter_space(self, strategy_name: str) -> dict[str, list[Any]]:
        strategy = self._load_strategy(strategy_name)
        return dict(getattr(strategy, "DEFAULT_PARAMETER_SPACE", {}))

    def _merged_strategy_parameters(
        self, strategy_name: str, parameters: dict[str, Any]
    ) -> dict[str, Any]:
        """Resolve one strategy's complete, versionable parameter set."""
        return {**self.default_parameters(strategy_name), **parameters}

    @staticmethod
    def _filter_signal_by_trade_mode(signal: int, parameters: dict[str, Any]) -> int:
        mode = str(parameters.get("trade_mode", "both")).strip().lower()
        if mode not in VALID_TRADE_MODES:
            raise ValueError(
                f"Invalid trade_mode {mode!r}; expected one of "
                f"{sorted(VALID_TRADE_MODES)}"
            )
        if mode == "long_only" and signal < 0:
            return 0
        if mode == "short_only" and signal > 0:
            return 0
        return signal

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
            base_parameters=None,
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
        current_position: int = 0,
        data_context: MarketDataContext | None = None,
    ) -> int:
        strategy = self._load_strategy(strategy_name)
        if not bars:
            return 0
        parameters = self._merged_strategy_parameters(strategy_name, parameters)
        if hasattr(strategy, "target_position_for_context") and data_context is not None:
            sig = int(strategy.target_position_for_context(
                bars, len(bars) - 1, parameters, current_position, data_context
            ))
        elif hasattr(strategy, "target_position_for_index"):
            sig = int(
                strategy.target_position_for_index(
                    bars, len(bars) - 1, parameters, current_position
                )
            )
        else:
            sig = int(strategy.signal_for_index(bars, len(bars) - 1, parameters))
        return self._filter_signal_by_trade_mode(sig, parameters)

    async def latest_signal_async(
        self,
        *,
        strategy_name: str,
        bars: list[BarRecord],
        parameters: dict[str, Any],
        current_position: int = 0,
        data_context: MarketDataContext | None = None,
    ) -> int:
        from functools import partial

        return await asyncio.get_event_loop().run_in_executor(
            self._executor,
            partial(
                self.latest_signal,
                strategy_name=strategy_name,
                bars=bars,
                parameters=parameters,
                current_position=current_position,
                data_context=data_context,
            ),
        )

    def _evaluate_parameter_space(
        self,
        *,
        bars: list[BarRecord],
        strategy_name: str,
        parameter_space: dict[str, list[Any]],
        base_parameters: dict[str, Any] | None = None,
        method: str = "ga",
        optimize_target: str = "sharpe",
        data_context: MarketDataContext | None = None,
    ) -> list[tuple[dict[str, Any], dict[str, float]]]:
        m = str(method).lower().strip()
        
        # Calculate total combinations
        total_combinations = (
            math.prod(len(v) for v in parameter_space.values()) if parameter_space else 0
        )
        should_run_exact = (
            total_combinations <= 1000
            or (
                m in ("heuristic", "bfs", "astar")
                and total_combinations <= EXACT_SEARCH_COMBINATION_LIMIT
            )
        )
        if should_run_exact:
            m = "grid"

        if m == "grid":
            return self._run_grid_search(
                bars, strategy_name, parameter_space, optimize_target,
                data_context, base_parameters,
            )
        elif m in ("heuristic", "bfs", "astar"):
            return self._run_heuristic_search(
                bars, strategy_name, parameter_space, optimize_target,
                data_context=data_context, base_parameters=base_parameters,
            )
        else:
            return self._run_genetic_search(
                bars, strategy_name, parameter_space, optimize_target,
                data_context=data_context, base_parameters=base_parameters,
            )

    def _run_grid_search(
        self,
        bars: list[BarRecord],
        strategy_name: str,
        parameter_space: dict[str, list[Any]],
        optimize_target: str = "sharpe",
        data_context: MarketDataContext | None = None,
        base_parameters: dict[str, Any] | None = None,
    ) -> list[tuple[dict[str, Any], dict[str, float]]]:
        keys = list(parameter_space.keys())
        value_lists = [parameter_space[k] for k in keys]
        combinations = list(itertools.product(*value_lists))
        
        evaluations = []
        for combo in combinations:
            params = dict(zip(keys, combo, strict=True))
            effective_parameters = {**(base_parameters or {}), **params}
            outcome = self._execute_with_context(
                bars, strategy_name, effective_parameters, data_context
            )
            evaluations.append((effective_parameters, outcome.metrics))
            
        return self._sort_evaluations(evaluations, optimize_target)

    def _run_heuristic_search(
        self,
        bars: list[BarRecord],
        strategy_name: str,
        parameter_space: dict[str, list[Any]],
        optimize_target: str = "sharpe",
        max_evaluations: int = 100,
        data_context: MarketDataContext | None = None,
        base_parameters: dict[str, Any] | None = None,
    ) -> list[tuple[dict[str, Any], dict[str, float]]]:
        """A*-inspired Heuristic graph search over parameter grid."""
        import heapq
        
        local_random = random.Random(42)
        keys = list(parameter_space.keys())
        dim_lengths = [len(parameter_space[k]) for k in keys]
        
        def node_to_params(node: tuple[int, ...]) -> dict[str, Any]:
            return {
                **(base_parameters or {}),
                **{keys[i]: parameter_space[keys[i]][node[i]] for i in range(len(keys))},
            }
            
        evaluations: dict[tuple[int, ...], tuple[dict[str, Any], dict[str, float]]] = {}
        
        def evaluate_node(node: tuple[int, ...]) -> float:
            if node in evaluations:
                _, metrics = evaluations[node]
            else:
                params = node_to_params(node)
                outcome = self._execute_with_context(bars, strategy_name, params, data_context)
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
                        
        return self._sort_evaluations(list(evaluations.values()), optimize_target)

    def _run_genetic_search(
        self,
        bars: list[BarRecord],
        strategy_name: str,
        parameter_space: dict[str, list[Any]],
        optimize_target: str = "sharpe",
        data_context: MarketDataContext | None = None,
        base_parameters: dict[str, Any] | None = None,
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
                    effective_parameters = {**(base_parameters or {}), **parameters}
                    outcome = self._execute_with_context(
                        bars, strategy_name, effective_parameters, data_context
                    )
                    evaluations[signature] = (effective_parameters, outcome.metrics)
                scored.append(evaluations[signature])
            # Sort by target primarily, penalizing zero trades to avoid passive dominance
            scored = self._sort_evaluations(scored, optimize_target)
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
        return self._sort_evaluations(list(evaluations.values()), optimize_target)

    def _sort_evaluations(
        self,
        evaluations: list[tuple[dict[str, Any], dict[str, float]]],
        optimize_target: str,
    ) -> list[tuple[dict[str, Any], dict[str, float]]]:
        target = str(optimize_target).lower().strip()

        def metric_value(metrics: dict[str, float], metric_name: str) -> float:
            if metrics.get("trade_count", 0) == 0:
                return -999.0
            return metrics.get(metric_name, 0.0)

        if target == "return":
            def key_fn(item: tuple[dict[str, Any], dict[str, float]]) -> tuple[float, float]:
                return (
                    metric_value(item[1], "total_return"),
                    metric_value(item[1], "sharpe_ratio"),
                )
        else:
            def key_fn(item: tuple[dict[str, Any], dict[str, float]]) -> tuple[float, float]:
                return (
                    metric_value(item[1], "sharpe_ratio"),
                    metric_value(item[1], "total_return"),
                )
        return sorted(evaluations, key=key_fn, reverse=True)

    def _execute_backtest(
        self,
        bars: list[BarRecord],
        strategy_name: str,
        parameters: dict[str, Any],
        *,
        decision_start_index: int = 0,
        data_context: MarketDataContext | None = None,
    ) -> BacktestOutcome:
        if not bars:
            return BacktestOutcome(metrics={}, equity_curve=[], signals=[])
        if decision_start_index < 0 or decision_start_index >= len(bars):
            raise ValueError("decision_start_index must point to a bar in the input")

        strategy = self._load_strategy(strategy_name)
        # Walk-forward evaluators supply only the parameters being searched.
        # Merge the complete strategy defaults here so fixed safety controls
        # (ATR sizing, execution governance, factor scales) apply identically
        # to backtests, optimization folds and runtime research.
        strategy_defaults = dict(getattr(strategy, "DEFAULT_PARAMETERS", {}))
        try:
            strategy_defaults = self.default_parameters(strategy_name)
        except (ImportError, ModuleNotFoundError):
            # Tests and third-party in-memory strategy doubles have no module.
            pass
        parameters = {**strategy_defaults, **parameters}
        # Direction selection belongs to this strategy parameter set.  It is
        # deliberately resolved before execution governance so a disallowed
        # reversal is treated as a close/flat signal, not as another strategy
        # direction hidden by a global setting.
        position = 0
        position_exposure = 1.0
        equity = [1.0]
        step_returns: list[float] = []
        signals: list[int] = []
        trades: list[TradeRecord] = []
        entry_price: float | None = None
        entry_time = None
        entry_index: int | None = None
        last_mark_price: float | None = None
        cooldown_until_index = 0
        min_holding_bars = max(0, int(parameters.get("min_holding_bars", 0)))
        cooldown_bars = max(0, int(parameters.get("cooldown_bars", 0)))
        atr_sizing_enabled = bool(parameters.get("enable_atr_sizing", False))
        atr_window = max(1, int(parameters.get("sizing_atr_window", 14)))
        sizer = AtrRiskSizer(
            risk_fraction=float(parameters.get("risk_fraction", 0.01)),
            stop_atr_multiple=float(parameters.get("stop_atr_multiple", 2.0)),
            max_notional_fraction=float(parameters.get("max_notional_fraction", 0.30)),
        ) if atr_sizing_enabled else None
        
        # Get fee rate from settings
        fee_rate = (
            self.settings.research.maker_fee_rate
            if self.settings.research.use_maker_fee
            else self.settings.research.taker_fee_rate
        )
        costs = CostModel(
            fee_rate=fee_rate,
            slippage_bps=getattr(self.settings.research, "slippage_bps", 0.0),
            spread_bps=getattr(self.settings.research, "spread_bps", 0.0),
            funding_rate_per_bar=getattr(self.settings.research, "funding_rate_per_bar", 0.0),
        )
        
        # Signal is calculated from a closed bar, then filled at the *next*
        # bar's open.  This prevents the optimistic same-close fill that the
        # original research loop used.
        for index in range(decision_start_index, len(bars) - 1):
            period_start_equity = equity[-1]
            period_end_equity = period_start_equity
            if hasattr(strategy, "target_position_for_context") and data_context is not None:
                signal = int(
                    strategy.target_position_for_context(
                        bars, index, parameters, position, data_context
                    )
                )
            elif hasattr(strategy, "target_position_for_index"):
                signal = int(
                    strategy.target_position_for_index(
                        bars, index, parameters, position
                    )
                )
            else:
                signal = int(strategy.signal_for_index(bars, index, parameters))

            signal = self._filter_signal_by_trade_mode(signal, parameters)

            # Generic execution governance is parameterised on the strategy
            # version so every plugin can opt in without duplicating stateful
            # fill logic. A reversal first closes the position, then observes
            # the cooldown before any new exposure is opened.
            if position == 0 and index + 1 < cooldown_until_index:
                signal = 0
            elif position != 0 and signal != position:
                assert entry_index is not None
                held_bars = index + 1 - entry_index
                if held_bars < min_holding_bars:
                    signal = position
                elif cooldown_bars:
                    signal = 0
                    cooldown_until_index = index + 1 + cooldown_bars
            
            signals.append(signal)

            fill_bar = bars[index + 1]
            raw_fill_price = fill_bar.open
            if last_mark_price is not None and position:
                period_end_equity *= 1 + position * position_exposure * (raw_fill_price / last_mark_price - 1)
                period_end_equity *= 1 - costs.funding_rate_per_bar * position_exposure

            if signal != position:
                if position != 0:
                    fill_price = costs.fill_price(raw_fill_price, side=-position)
                    period_end_equity *= 1 - costs.execution_cost_rate * position_exposure
                    assert entry_price is not None and entry_time is not None and entry_index is not None
                    gross_return = position_exposure * position * (fill_price / entry_price - 1)
                    transaction_cost = 2 * costs.execution_cost_rate * position_exposure
                    funding_cost = costs.funding_rate_per_bar * position_exposure * (index + 1 - entry_index)
                    net_return = (1 + gross_return) * (1 - 2 * costs.fee_rate * position_exposure) - 1 - funding_cost
                    trades.append(TradeRecord(
                        direction="long" if position > 0 else "short", entry_time=entry_time,
                        exit_time=fill_bar.datetime, entry_price=entry_price, exit_price=fill_price,
                        gross_return=gross_return, net_return=net_return, bars_held=index + 1 - entry_index,
                        transaction_cost=transaction_cost, funding_cost=funding_cost,
                    ))
                
                if signal != 0:
                    next_exposure = 1.0
                    if sizer is not None:
                        atr_start = max(0, index - atr_window + 1)
                        true_ranges = [
                            bars[item].high - bars[item].low
                            if item == 0
                            else max(
                                bars[item].high - bars[item].low,
                                abs(bars[item].high - bars[item - 1].close),
                                abs(bars[item].low - bars[item - 1].close),
                            )
                            for item in range(atr_start, index + 1)
                        ]
                        atr = sum(true_ranges) / len(true_ranges) if true_ranges else 0.0
                        decision = sizer.size(equity=period_end_equity, price=raw_fill_price, atr=atr)
                        next_exposure = decision.notional / period_end_equity if period_end_equity else 0.0
                    if next_exposure <= 0:
                        signal = 0
                    else:
                        position_exposure = next_exposure
                if signal != 0:
                    fill_price = costs.fill_price(raw_fill_price, side=signal)
                    period_end_equity *= 1 - costs.execution_cost_rate * position_exposure
                    entry_price, entry_time, entry_index = fill_price, fill_bar.datetime, index + 1

                position = signal
                if position == 0:
                    position_exposure = 1.0
            last_mark_price = raw_fill_price
            # One primary-bar transition produces exactly one net return. Price
            # movement, funding, spread, slippage and fees are combined here;
            # flat periods remain explicit zero observations for valid
            # volatility/annualisation statistics.
            step_returns.append(
                period_end_equity / period_start_equity - 1
                if period_start_equity > 0
                else 0.0
            )
            equity.append(period_end_equity)
            
        # Close final position if any to account for exit fees
        if position != 0:
            final_bar = bars[-1]
            equity[-1] *= 1 + position * position_exposure * (final_bar.close / (last_mark_price or final_bar.open) - 1)
            exit_price = costs.fill_price(final_bar.close, side=-position)
            equity[-1] *= 1 - costs.execution_cost_rate * position_exposure
            # The final open-to-close mark and liquidation belong to the last
            # primary-bar transition rather than to artificial extra periods.
            if len(equity) >= 2 and step_returns:
                prior_equity = equity[-2]
                step_returns[-1] = equity[-1] / prior_equity - 1 if prior_equity > 0 else 0.0
            assert entry_price is not None and entry_time is not None and entry_index is not None
            gross_return = position_exposure * position * (exit_price / entry_price - 1)
            transaction_cost = 2 * costs.execution_cost_rate * position_exposure
            funding_cost = costs.funding_rate_per_bar * position_exposure * (len(bars) - 1 - entry_index)
            net_return = (1 + gross_return) * (1 - 2 * costs.fee_rate * position_exposure) - 1 - funding_cost
            trades.append(TradeRecord(
                direction="long" if position > 0 else "short", entry_time=entry_time,
                exit_time=final_bar.datetime, entry_price=entry_price, exit_price=exit_price,
                gross_return=gross_return, net_return=net_return, bars_held=len(bars) - 1 - entry_index,
                transaction_cost=transaction_cost, funding_cost=funding_cost,
            ))

        interval = bars[0].interval
        metrics = self._metrics_from_returns(step_returns, equity, len(trades), interval)
        metrics.update(self._trade_metrics(trades))
        metrics["turnover"] = round(sum(abs(trade.gross_return) for trade in trades), 6)
        return BacktestOutcome(metrics=metrics, equity_curve=equity, signals=signals, trades=trades)

    def _execute_with_context(
        self,
        bars: list[BarRecord],
        strategy_name: str,
        parameters: dict[str, Any],
        data_context: MarketDataContext | None,
    ) -> BacktestOutcome:
        """Keep direct test/plugin replacements backward compatible."""
        if data_context is None:
            return self._execute_backtest(bars, strategy_name, parameters)
        return self._execute_backtest(
            bars, strategy_name, parameters, data_context=data_context
        )

    @staticmethod
    def _trade_metrics(trades: list[TradeRecord]) -> dict[str, float]:
        if not trades:
            return {"trade_count": 0.0, "win_rate": 0.0, "profit_factor": 0.0}
        returns = [trade.net_return for trade in trades]
        wins = [value for value in returns if value > 0]
        losses = [value for value in returns if value < 0]
        profit_factor = sum(wins) / abs(sum(losses)) if losses else (99.9 if wins else 0.0)
        return {
            "trade_count": float(len(trades)),
            "win_rate": round(len(wins) / len(trades), 4),
            "profit_factor": round(profit_factor, 4),
        }

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
