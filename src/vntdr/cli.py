from __future__ import annotations

import importlib
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import redis
import typer

from vntdr.adapters.orders import OkxOrderExecutor, SimulatedOrderExecutor
from vntdr.adapters.state import RedisSignalStore
from vntdr.adapters.telegram import TelegramNotifier
from vntdr.config import Settings
from vntdr.models import (
    HealthCheckResult, Instrument, Interval, MonitorResult, ResearchJobConfig, ShadowRun,
    StrategyActivation, StrategyInstance, StrategyVersion, SyncResult,
)
from vntdr.services.history import HistorySyncService, OkxHistoryClient
from vntdr.services.tradingview_history import TradingViewHistoryClient
from vntdr.services.external_factors import OkxDerivativesProvider
from vntdr.services.factor_sync import FactorSyncService
from vntdr.services.akshare_fund_flow import (
    AkShareDataError,
    AkShareFlowConfig,
    AkShareFundFlowProvider,
    AkShareUnavailableError,
    month_bounds,
)
from vntdr.services.etf_flow_ingestion import (
    EtfFlowIngestionService,
    parse_watchlist,
)
from vntdr.services.etf_flow_scheduler import EtfFlowScheduler
from vntdr.services.governance import StrategyGovernanceService
from vntdr.services.monitoring import MonitoringService
from vntdr.services.portfolio_runtime import PortfolioRuntimeService
from vntdr.services.research import ResearchService
from vntdr.services.risk import RiskManager
from vntdr.services.strategy_runtime import StrategyRuntimeService
from vntdr.services.telegram_research import TelegramResearchService
from vntdr.storage.database import Database
from vntdr.storage.repositories import (
    EtfMoneyFlowRepository,
    MarketDataRepository,
    ResearchRunRepository,
    StrategyRepository,
)

app = typer.Typer(add_completion=False, no_args_is_help=True)


DEFAULT_GRADIO_PORT = 7860


def _resolve_gradio_port(port: int | None) -> int:
    if port is not None:
        return port
    raw_port = os.getenv("GRADIO_PORT")
    if raw_port is None or raw_port == "":
        return DEFAULT_GRADIO_PORT
    try:
        return int(raw_port)
    except ValueError as exc:
        raise typer.BadParameter("GRADIO_PORT must be an integer") from exc


class CommandContext:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.database = Database(settings.database.dsn)
        self.database.create_schema()
        self.market_data_repository = MarketDataRepository(self.database)
        self.research_run_repository = ResearchRunRepository(self.database)
        self.strategy_repository = StrategyRepository(self.database)
        import threading
        self._runtime_config_lock = threading.Lock()
        self.history_service = HistorySyncService(
            settings=settings,
            history_client=self._build_history_client(settings),
            market_data_repository=self.market_data_repository,
            research_run_repository=self.research_run_repository,
        )
        self.research_service = ResearchService(
            settings=settings,
            market_data_repository=self.market_data_repository,
            research_run_repository=self.research_run_repository,
            factor_repository=self.strategy_repository,
        )
        redis_client = redis.from_url(settings.redis.url)
        order_executor = self._build_order_executor(settings)
        self.monitoring_service = MonitoringService(
            research_service=self.research_service,
            market_data_repository=self.market_data_repository,
            notifier=TelegramNotifier(
                bot_token=settings.telegram.bot_token.get_secret_value() if settings.telegram.bot_token else "",
                chat_id=settings.telegram.chat_id or "",
            ),
            order_executor=order_executor,
            signal_store=RedisSignalStore(redis_client),
            risk_manager=RiskManager(settings.risk),
        )
        self.strategy_runtime = StrategyRuntimeService(self.strategy_repository)
        self.strategy_governance = StrategyGovernanceService(self.strategy_repository)
        self.portfolio_runtime = PortfolioRuntimeService(
            strategy_repository=self.strategy_repository,
            strategy_runtime=self.strategy_runtime,
            monitoring_service=self.monitoring_service,
        )
        self._okx_runtime_signature = self._okx_runtime_config_signature(settings)
        self.telegram_research_service = TelegramResearchService(
            settings=settings,
            history_service=self.history_service,
            research_service=self.research_service,
        )

    def _build_history_client(self, settings: Settings) -> OkxHistoryClient:
        return OkxHistoryClient(
            base_url=settings.okx.rest_base_url,
            demo_trading=settings.okx.demo_trading,
        )

    def _build_order_executor(self, settings: Settings):
        if not settings.okx.trading_enabled:
            return SimulatedOrderExecutor()
        return OkxOrderExecutor(
            api_key=settings.okx.api_key.get_secret_value() if settings.okx.api_key else "",
            secret_key=settings.okx.secret_key.get_secret_value() if settings.okx.secret_key else "",
            passphrase=settings.okx.passphrase.get_secret_value() if settings.okx.passphrase else "",
            demo_trading=settings.okx.demo_trading,
            margin_mode=settings.okx.margin_mode,
            order_type=settings.okx.order_type,
            order_retry_count=settings.okx.order_retry_count,
            order_retry_wait_seconds=settings.okx.order_retry_wait_seconds,
        )

    def _okx_runtime_config_signature(self, settings: Settings) -> tuple[Any, ...]:
        return (
            settings.okx.api_key.get_secret_value() if settings.okx.api_key else "",
            settings.okx.secret_key.get_secret_value() if settings.okx.secret_key else "",
            settings.okx.passphrase.get_secret_value() if settings.okx.passphrase else "",
            settings.okx.demo_trading,
            settings.okx.rest_base_url,
            settings.okx.margin_mode,
            settings.okx.order_type,
            settings.okx.order_retry_count,
            settings.okx.order_retry_wait_seconds,
        )

    def refresh_runtime_config(self, config_service: Any | None = None) -> None:
        if config_service is None:
            from vntdr.services.config_service import ConfigService
            config_service = ConfigService(self.settings)
        with self._runtime_config_lock:
            config_service._load_overrides()

            signature = self._okx_runtime_config_signature(self.settings)
            if signature == self._okx_runtime_signature:
                return

            self.monitoring_service.order_executor = self._build_order_executor(self.settings)
            self.history_service.history_client = self._build_history_client(self.settings)
            self._okx_runtime_signature = signature
            import logging
            logging.getLogger(__name__).info(
                "Reloaded OKX runtime clients after configuration change "
                "(demo_trading=%s, trading_enabled=%s)",
                self.settings.okx.demo_trading,
                self.settings.okx.trading_enabled,
            )

    def doctor(self) -> HealthCheckResult:
        checks: dict[str, bool] = {}
        details: dict[str, str] = {}
        try:
            self.database.ping()
            checks["database"] = True
        except Exception as exc:
            checks["database"] = False
            details["database"] = str(exc)

        try:
            redis_client = redis.from_url(self.settings.redis.url)
            redis_client.ping()
            checks["redis"] = True
        except Exception as exc:
            checks["redis"] = False
            details["redis"] = str(exc)

        try:
            for package_name in ("vnpy", "vnpy_ctastrategy", "vnpy_okx", "vnpy_postgresql", "vnpy_riskmanager"):
                importlib.import_module(package_name)
            checks["veighna"] = True
        except Exception as exc:
            checks["veighna"] = False
            details["veighna"] = str(exc)

        return HealthCheckResult(ok=all(checks.values()), checks=checks, details=details)

    def sync_history(self, **kwargs: Any) -> SyncResult:
        return self.history_service.sync(**kwargs)

    def backtest(self, config: ResearchJobConfig):
        return self.research_service.backtest(config)

    def optimize(self, config: ResearchJobConfig, method: str):
        return self.research_service.optimize(config, method=method)

    def walk_forward(self, config: ResearchJobConfig):
        return self.research_service.walk_forward(config)

    def validate_candidate(self, *, backtest_config: ResearchJobConfig, walk_forward_config: ResearchJobConfig):
        return self.research_service.validate_candidate(
            backtest_config=backtest_config,
            walk_forward_config=walk_forward_config,
        )

    def factor_ablation(self, config: ResearchJobConfig, variants: dict[str, dict[str, Any]]):
        return self.research_service.factor_ablation(config, variants)

    def telegram_research(self) -> TelegramResearchService:
        return self.telegram_research_service

    def create_strategy_instance(
        self, *, name: str, strategy_name: str, symbol: str, exchange: str,
        asset_class: str, interval: str, execution_mode: str = "notify_only",
        parameters: dict[str, Any] | None = None,
        auxiliary_intervals: list[str] | None = None,
    ) -> tuple[StrategyInstance, StrategyVersion]:
        # Ensure a misspelled plugin cannot be persisted as an executable instance.
        self.research_service._load_strategy(strategy_name)
        instance = self.strategy_repository.create_instance(StrategyInstance(
            name=name, instrument=Instrument(symbol=symbol, exchange=exchange, asset_class=asset_class),
            primary_interval=Interval(value=interval),
            auxiliary_intervals=[Interval(value=value) for value in (auxiliary_intervals or [])],
            execution_mode=execution_mode,
        ))
        version = self.strategy_repository.create_version(StrategyVersion(
            strategy_name=strategy_name, parameters=parameters or self.research_service.default_parameters(strategy_name),
        ))
        return instance, version

    def approve_strategy_version(
        self,
        *,
        instance_id: UUID,
        version_id: UUID,
        approved_by: str,
        backtest_passed: bool,
        walk_forward_passed: bool,
        shadow_passed: bool,
        max_drawdown: float | None,
    ) -> StrategyActivation:
        from vntdr.models import ValidationGate
        return self.strategy_governance.approve_activation(
            instance_id=instance_id,
            version_id=version_id,
            effective_at=datetime.now().astimezone(),
            approved_by=approved_by,
            validation=ValidationGate(
                backtest_passed=backtest_passed,
                walk_forward_passed=walk_forward_passed,
                shadow_passed=shadow_passed,
                max_drawdown=max_drawdown,
            ),
        )

    def rollback_strategy_version(
        self, *, instance_id: UUID, target_version_id: UUID, approved_by: str
    ) -> StrategyActivation:
        return self.strategy_governance.rollback(
            instance_id=instance_id,
            target_version_id=target_version_id,
            effective_at=datetime.now().astimezone(),
            approved_by=approved_by,
        )

    def run_portfolio_once(self, *, volume: float | None = None):
        self.refresh_runtime_config()
        return self.portfolio_runtime.run_enabled(
            volume=volume if volume is not None else self.settings.research.default_order_size,
            lookback_bars=self.settings.research.monitor_lookback_bars,
        )

    def monitor_once(
        self,
        *,
        strategy_name: str,
        symbol: str,
        interval: str,
        method: str,
        volume: float,
        parameters: dict[str, Any] | None = None,
        parameter_space: dict[str, list[Any]] | None = None,
    ) -> MonitorResult:
        self.refresh_runtime_config()
        return self.monitoring_service.monitor_once(
            strategy_name=strategy_name,
            symbol=symbol,
            interval=interval,
            parameters=parameters,
            parameter_space=parameter_space,
            volume=volume,
            method=method,
            lookback_bars=self.settings.research.monitor_lookback_bars,
        )

    async def monitor_once_async(
        self,
        *,
        strategy_name: str,
        symbol: str,
        interval: str,
        method: str,
        volume: float,
        parameters: dict[str, Any] | None = None,
        parameter_space: dict[str, list[Any]] | None = None,
    ) -> MonitorResult:
        self.refresh_runtime_config()
        return await self.monitoring_service.monitor_once_async(
            strategy_name=strategy_name,
            symbol=symbol,
            interval=interval,
            parameters=parameters,
            parameter_space=parameter_space,
            volume=volume,
            method=method,
            lookback_bars=self.settings.research.monitor_lookback_bars,
        )


def create_command_context(settings: Settings) -> CommandContext:
    return CommandContext(settings)


@app.command("strategy-create")
def strategy_create_command(
    name: str = typer.Option(...),
    strategy: str = typer.Option(...),
    symbol: str = typer.Option(...),
    interval: str = typer.Option(...),
    exchange: str = typer.Option("OKX"),
    asset_class: str = typer.Option("crypto"),
    execution_mode: str = typer.Option("notify_only"),
    auxiliary_interval: list[str] = typer.Option([], "--aux-interval", help="可重复，例如 --aux-interval 1d"),
) -> None:
    """Register a versioned strategy instance; safe notification-only by default."""
    settings = Settings.from_env()
    settings.validate_for("backtest")
    instance, version = create_command_context(settings).create_strategy_instance(
        name=name, strategy_name=strategy, symbol=symbol, exchange=exchange,
        asset_class=asset_class, interval=interval, execution_mode=execution_mode,
        auxiliary_intervals=auxiliary_interval,
    )
    typer.echo(f"strategy instance created: {instance.id} ({instance.name})")
    typer.echo(f"pending approval version: {version.id}")


@app.command("strategy-approve")
def strategy_approve_command(
    instance_id: str = typer.Option(...),
    version_id: str = typer.Option(...),
    approved_by: str = typer.Option(...),
    backtest_run_id: int = typer.Option(..., help="已完成的回测研究运行 ID"),
    walk_forward_run_id: int = typer.Option(..., help="已完成的走查研究运行 ID"),
    shadow_run_id: str | None = typer.Option(None, help="已完成且通过的影子运行 ID"),
) -> None:
    """Approve a validated version for activation on its next runtime check."""
    settings = Settings.from_env()
    settings.validate_for("backtest")
    context = create_command_context(settings)
    instance = context.strategy_repository.get_instance(instance_id)
    if instance is None:
        raise typer.BadParameter("unknown strategy instance")
    with context.database.session() as session:
        from vntdr.storage.database import StrategyVersionORM
        version_row = session.get(StrategyVersionORM, version_id)
    if version_row is None:
        raise typer.BadParameter("unknown strategy version")
    expected = (version_row.strategy_name, instance.instrument.symbol, instance.primary_interval.value)
    evidence = []
    for run_id, expected_mode in ((backtest_run_id, "backtest"), (walk_forward_run_id, "walk-forward")):
        found = context.research_run_repository.get_research_run(run_id)
        if found is None:
            raise typer.BadParameter(f"unknown research run: {run_id}")
        report, _, status = found
        if status != "completed" or report.mode != expected_mode:
            raise typer.BadParameter(f"research run {run_id} is not a completed {expected_mode} run")
        if (report.strategy_name, report.symbol, report.interval) != expected:
            raise typer.BadParameter(f"research run {run_id} does not match the instance/version dataset")
        evidence.append(report)
    backtest, walk_forward = evidence
    if backtest.metrics.get("trade_count", 0) < 10:
        raise typer.BadParameter("backtest evidence has fewer than 10 trades")
    if (len(walk_forward.fold_results) < 3 or walk_forward.metrics.get("total_return", 0) <= 0
            or walk_forward.metrics.get("max_drawdown", 0) < -0.10):
        raise typer.BadParameter("walk-forward evidence does not pass folds, return, or drawdown gates")
    if not shadow_run_id:
        raise typer.BadParameter("--shadow-run-id is required; a boolean flag is not shadow evidence")
    shadow = context.strategy_repository.get_shadow_run(shadow_run_id)
    if shadow is None or shadow.status != "passed":
        raise typer.BadParameter("shadow run must exist and have status=passed")
    if shadow.instance_id != UUID(instance_id) or shadow.strategy_version_id != UUID(version_id):
        raise typer.BadParameter("shadow run must belong to the instance and version being approved")
    activation = context.approve_strategy_version(
        instance_id=UUID(instance_id), version_id=UUID(version_id), approved_by=approved_by,
        backtest_passed=True, walk_forward_passed=True,
        shadow_passed=True, max_drawdown=shadow.max_drawdown,
    )
    typer.echo(f"strategy version activated: {activation.strategy_version_id}")


@app.command("strategy-rollback")
def strategy_rollback_command(
    instance_id: str = typer.Option(...),
    target_version_id: str = typer.Option(...),
    approved_by: str = typer.Option(...),
) -> None:
    """Roll back an instance to a previously approved version."""
    settings = Settings.from_env()
    settings.validate_for("backtest")
    activation = create_command_context(settings).rollback_strategy_version(
        instance_id=UUID(instance_id), target_version_id=UUID(target_version_id), approved_by=approved_by,
    )
    typer.echo(f"rolled back to version: {activation.strategy_version_id}")


@app.command("shadow-start")
def shadow_start_command(
    instance_id: str = typer.Option(...),
    version_id: str = typer.Option(...),
    initial_equity: float = typer.Option(1.0),
) -> None:
    """Start an auditable notification-only observation run for one version."""
    settings = Settings.from_env()
    settings.validate_for("backtest")
    run = create_command_context(settings).strategy_repository.create_shadow_run(ShadowRun(
        instance_id=UUID(instance_id), strategy_version_id=UUID(version_id), initial_equity=initial_equity,
    ))
    typer.echo(f"shadow run started: {run.id}")


@app.command("shadow-record-equity")
def shadow_record_equity_command(
    shadow_run_id: str = typer.Option(...),
    equity: float = typer.Option(...),
    observed_at: str | None = typer.Option(None),
) -> None:
    """Append a marked-to-market equity observation to an active shadow run."""
    settings = Settings.from_env()
    settings.validate_for("backtest")
    at = datetime.fromisoformat(observed_at) if observed_at else datetime.now().astimezone()
    run = create_command_context(settings).strategy_repository.record_shadow_equity(
        shadow_run_id, equity, at,
    )
    typer.echo(f"shadow observations={run.observation_count} drawdown={run.max_drawdown:.2%}")


@app.command("shadow-finish")
def shadow_finish_command(
    shadow_run_id: str = typer.Option(...),
    status: str = typer.Option(..., help="passed or failed"),
) -> None:
    """Finalize a shadow run after its externally reviewed observation period."""
    settings = Settings.from_env()
    settings.validate_for("backtest")
    run = create_command_context(settings).strategy_repository.finalize_shadow_run(shadow_run_id, status)
    typer.echo(f"shadow run {run.id}: {run.status}, drawdown={run.max_drawdown:.2%}")


@app.command("portfolio-run")
def portfolio_run_command(volume: float | None = typer.Option(None)) -> None:
    """Run all enabled versioned instances and print the notification-only target portfolio."""
    settings = Settings.from_env()
    settings.validate_for("live")
    result = create_command_context(settings).run_portfolio_once(volume=volume)
    typer.echo(f"gross={result.portfolio.gross_exposure:.4f} net={result.portfolio.net_exposure:.4f}")
    for symbol, weight in result.portfolio.target_weights.items():
        typer.echo(f"{symbol}: {weight:.4f}")
    for reason in result.portfolio.scaling_reasons:
        typer.echo(f"scaled: {reason}")
    for instance, error in result.errors.items():
        typer.echo(f"error {instance}: {error}")


def run() -> None:
    app()


@app.command("doctor")
def doctor_command() -> None:
    settings = Settings.from_env()
    try:
        result = create_command_context(settings).doctor()
    except Exception as exc:
        result = HealthCheckResult(
            ok=False,
            checks={"database": False, "redis": False, "veighna": False},
            details={"database": str(exc)},
        )
    for line in result.lines():
        typer.echo(line)
    raise typer.Exit(code=0 if result.ok else 1)


@app.command("sync-history")
def sync_history_command(
    symbol: str = typer.Option(...),
    interval: str = typer.Option(...),
    start: str = typer.Option(...),
    end: str = typer.Option(...),
    fill_missing: bool = typer.Option(False),
    calendar: str = typer.Option("continuous", help="continuous 或 weekday"),
) -> None:
    settings = Settings.from_env()
    settings.validate_for("sync-history")
    context = create_command_context(settings)
    result = context.sync_history(
        symbol=symbol,
        interval=interval,
        start=datetime.fromisoformat(start),
        end=datetime.fromisoformat(end),
        fill_missing=fill_missing,
        calendar=calendar,
    )
    typer.echo(
        f"sync job={result.job_id} inserted={result.inserted_count} cleaned={result.cleaned_count} "
        f"duplicates={result.duplicates_removed}"
    )


@app.command("sync-tradingview")
def sync_tradingview_command(
    tradingview_symbol: str = typer.Option(..., "--tv-symbol", help="例如 OANDA:XAUUSD"),
    output_symbol: str = typer.Option(..., help="必须以 TV: 开头，例如 TV:XAUUSD"),
    interval: str = typer.Option(...),
    start: str = typer.Option(...),
    end: str = typer.Option(...),
    fill_missing: bool = typer.Option(False),
    extended_session: bool = typer.Option(False),
    calendar: str = typer.Option("weekday", help="TradingView 代理默认按交易日市场处理"),
) -> None:
    """通过 TradingView 非官方 WebSocket 拉取隔离的研究代理行情。"""
    settings = Settings.from_env()
    settings.validate_for("sync-history")
    context = create_command_context(settings)
    service = HistorySyncService(
        settings=settings,
        history_client=TradingViewHistoryClient(
            tradingview_symbol=tradingview_symbol,
            output_symbol=output_symbol,
            auth_token=os.getenv("TRADINGVIEW_AUTH_TOKEN"),
            extended_session=extended_session,
        ),
        market_data_repository=context.market_data_repository,
        research_run_repository=context.research_run_repository,
    )
    result = service.sync(
        symbol=output_symbol,
        interval=interval,
        start=datetime.fromisoformat(start),
        end=datetime.fromisoformat(end),
        fill_missing=fill_missing,
        calendar=calendar,
    )
    typer.echo(
        f"TradingView research sync job={result.job_id} symbol={output_symbol} "
        f"inserted={result.inserted_count} cleaned={result.cleaned_count} "
        f"duplicates={result.duplicates_removed}"
    )


@app.command("sync-okx-derivatives")
def sync_okx_derivatives_command(
    symbol: str = typer.Option(...),
    asset_class: str = typer.Option("crypto"),
    start: str = typer.Option(...),
    end: str = typer.Option(...),
) -> None:
    """Sync public OKX funding/open-interest observations for factor research."""
    settings = Settings.from_env()
    settings.validate_for("sync-history")
    context = create_command_context(settings)
    instrument = Instrument(symbol=symbol, exchange="OKX", asset_class=asset_class)
    count = FactorSyncService(repository=context.strategy_repository).sync(
        provider=OkxDerivativesProvider(base_url=settings.okx.rest_base_url),
        instrument=instrument,
        start=datetime.fromisoformat(start),
        end=datetime.fromisoformat(end),
    )
    typer.echo(f"OKX derivative factors synced: {count} observations for {symbol}")


@app.command("akshare-csi300-flow")
def akshare_csi300_flow_command(
    symbol: str | None = typer.Option(
        None, help="单标的验证，例如 515180；设置后不抓取沪深300"
    ),
    market: str = typer.Option("sh", help="单标的市场：sh、sz 或 bj"),
    start: str | None = typer.Option(None, "--from", help="开始日期 YYYY-MM-DD，默认本月1日"),
    end: str | None = typer.Option(None, "--to", help="结束日期 YYYY-MM-DD，默认今天"),
    max_stocks: int | None = typer.Option(
        None, min=1, help="最多抓取多少只成分股；MVP 小额验证可设为 10"
    ),
    output_dir: Path | None = typer.Option(
        None, help="结果目录，默认 VNTDR_REPORT_DIR 或 reports"
    ),
    request_interval: float = typer.Option(
        0.8, min=0.0, help="个股请求之间的等待秒数，避免公开接口限流"
    ),
    max_retries: int = typer.Option(3, min=0, help="单只股票失败后的重试次数"),
    retry_backoff_seconds: float = typer.Option(
        1.0, min=0.0, help="重试初始退避秒数，之后按 2 倍递增"
    ),
    retry_jitter_seconds: float = typer.Option(
        0.25, min=0.0, help="重试退避的随机抖动秒数，避免请求同时重试"
    ),
) -> None:
    """用 AkShare 拉取沪深300本月个股主力/大单资金流趋势。"""
    settings = Settings.from_env()
    default_start, default_end = month_bounds()
    try:
        start_date = date.fromisoformat(start) if start else default_start
        end_date = date.fromisoformat(end) if end else default_end
    except ValueError as exc:
        raise typer.BadParameter("日期必须是 YYYY-MM-DD") from exc
    if start_date > end_date:
        raise typer.BadParameter("开始日期不能晚于结束日期")

    provider = AkShareFundFlowProvider(
        config=AkShareFlowConfig(
            request_interval_seconds=request_interval,
            max_retries=max_retries,
            retry_backoff_seconds=retry_backoff_seconds,
            retry_jitter_seconds=retry_jitter_seconds,
        )
    )
    try:
        if symbol:
            daily, stock, summary = provider.fetch_symbol(
                symbol=symbol,
                market=market,
                start_date=start_date,
                end_date=end_date,
            )
        else:
            daily, stock, summary = provider.fetch_month(
                start_date=start_date,
                end_date=end_date,
                max_stocks=max_stocks,
            )
    except (AkShareDataError, AkShareUnavailableError) as exc:
        typer.echo(f"AkShare 数据获取失败：{exc}", err=True)
        raise typer.Exit(code=2) from exc

    destination = output_dir or settings.research.report_dir
    destination.mkdir(parents=True, exist_ok=True)
    scope = str(symbol).zfill(6) if symbol else "csi300"
    slug = f"akshare_{scope}_flow_{start_date.isoformat()}_{end_date.isoformat()}"
    daily_path = destination / f"{slug}_daily.csv"
    stock_path = destination / f"{slug}_stocks.csv"
    summary_path = destination / f"{slug}_summary.json"
    daily.to_csv(daily_path, index=False)
    stock.to_csv(stock_path, index=False)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    typer.echo(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    typer.echo(f"daily_csv={daily_path}")
    typer.echo(f"stocks_csv={stock_path}")
    typer.echo(f"summary_json={summary_path}")
    if not stock.empty:
        columns = [
            "symbol", "main_net_inflow_sum", "large_net_inflow_sum",
            "main_positive_ratio", "latest_main_net_inflow",
        ]
        typer.echo("top_stocks=")
        typer.echo(stock.loc[:, [column for column in columns if column in stock]].head(10).to_csv(index=False))


def _build_etf_flow_ingestion_service(
    settings: Settings,
    *,
    symbols: str | None,
    lookback_days: int,
    request_interval: float,
    max_retries: int,
    retry_backoff_seconds: float,
    retry_jitter_seconds: float,
    timezone_name: str = "Asia/Shanghai",
    availability_hour: int = 16,
    availability_minute: int = 10,
) -> EtfFlowIngestionService:
    raw_watchlist = symbols or os.getenv("VNTDR_ETF_WATCHLIST")
    watchlist = parse_watchlist(raw_watchlist)
    database = Database(settings.database.dsn)
    database.create_schema()
    provider = AkShareFundFlowProvider(
        config=AkShareFlowConfig(
            request_interval_seconds=request_interval,
            max_retries=max_retries,
            retry_backoff_seconds=retry_backoff_seconds,
            retry_jitter_seconds=retry_jitter_seconds,
        )
    )
    return EtfFlowIngestionService(
        provider=provider,
        repository=EtfMoneyFlowRepository(database),
        watchlist=watchlist,
        lookback_days=lookback_days,
        timezone_name=timezone_name,
        availability_hour=availability_hour,
        availability_minute=availability_minute,
    )


def _parse_optional_date(value: str | None, *, label: str) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter(f"{label}必须是 YYYY-MM-DD") from exc


@app.command("etf-flow-ingest")
def etf_flow_ingest_command(
    symbols: str | None = typer.Option(
        None,
        help=(
            "观察池，格式 symbol:market,symbol:market；默认使用指定的 7 只 ETF，"
            "也可用 VNTDR_ETF_WATCHLIST 覆盖"
        ),
    ),
    start: str | None = typer.Option(None, "--from", help="开始日期 YYYY-MM-DD"),
    end: str | None = typer.Option(None, "--to", help="结束日期 YYYY-MM-DD，默认今天"),
    lookback_days: int = typer.Option(120, min=1, help="未指定开始日期时回溯的自然日天数"),
    output_dir: Path | None = typer.Option(None, help="摘要目录，默认 VNTDR_REPORT_DIR 或 reports"),
    request_interval: float = typer.Option(
        0.8, min=0.0, help="个股请求之间的等待秒数，避免公开接口限流"
    ),
    max_retries: int = typer.Option(3, min=0, help="单只 ETF 失败后的重试次数"),
    retry_backoff_seconds: float = typer.Option(
        1.0, min=0.0, help="重试初始退避秒数，之后按 2 倍递增"
    ),
    retry_jitter_seconds: float = typer.Option(
        0.25, min=0.0, help="重试退避的随机抖动秒数"
    ),
) -> None:
    """一次性拉取 ETF 观察池资金流并幂等写入 PostgreSQL。"""
    settings = Settings.from_env()
    settings.validate_for("etf-flow-ingest")
    start_date = _parse_optional_date(start, label="开始日期")
    end_date = _parse_optional_date(end, label="结束日期")
    if start_date and end_date and start_date > end_date:
        raise typer.BadParameter("开始日期不能晚于结束日期")
    try:
        service = _build_etf_flow_ingestion_service(
            settings,
            symbols=symbols,
            lookback_days=lookback_days,
            request_interval=request_interval,
            max_retries=max_retries,
            retry_backoff_seconds=retry_backoff_seconds,
            retry_jitter_seconds=retry_jitter_seconds,
        )
        result = service.run(start_date=start_date, end_date=end_date)
    except (AkShareDataError, AkShareUnavailableError, ValueError) as exc:
        typer.echo(f"ETF 资金流采集失败：{exc}", err=True)
        raise typer.Exit(code=2) from exc
    destination = output_dir or settings.research.report_dir
    destination.mkdir(parents=True, exist_ok=True)
    summary_path = destination / f"etf_flow_ingest_{result['date_end']}.json"
    summary_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    typer.echo(f"summary_json={summary_path}")
    if result.get("retryable", False):
        raise typer.Exit(code=1)


@app.command("etf-flow-scheduler")
def etf_flow_scheduler_command(
    run_once: bool = typer.Option(False, help="立即执行一次后退出，不启动常驻调度"),
    symbols: str | None = typer.Option(None, help="观察池，格式 symbol:market,symbol:market"),
    hour: int = typer.Option(16, min=0, max=23, help="每日触发小时（Asia/Shanghai）"),
    minute: int = typer.Option(10, min=0, max=59, help="每日触发分钟（Asia/Shanghai）"),
    timezone_name: str = typer.Option("Asia/Shanghai", help="调度时区"),
    lookback_days: int = typer.Option(120, min=1),
    request_interval: float = typer.Option(0.8, min=0.0),
    max_retries: int = typer.Option(3, min=0),
    retry_backoff_seconds: float = typer.Option(1.0, min=0.0),
    retry_jitter_seconds: float = typer.Option(0.25, min=0.0),
    retry_until_success: bool = typer.Option(
        True,
        "--retry-until-success/--no-retry-until-success",
        help="任务失败后持续延迟重试，直到整批成功",
    ),
    task_retry_base_seconds: float = typer.Option(
        60.0, min=1.0, help="任务级重试的初始等待秒数"
    ),
    task_retry_max_seconds: float = typer.Option(
        1800.0, min=1.0, help="任务级重试的最大等待秒数"
    ),
) -> None:
    """使用 APScheduler 在交易日收盘后持续采集 ETF 资金流。"""
    settings = Settings.from_env()
    settings.validate_for("etf-flow-scheduler")
    try:
        service = _build_etf_flow_ingestion_service(
            settings,
            symbols=symbols,
            lookback_days=lookback_days,
            request_interval=request_interval,
            max_retries=max_retries,
            retry_backoff_seconds=retry_backoff_seconds,
            retry_jitter_seconds=retry_jitter_seconds,
            timezone_name=timezone_name,
            availability_hour=hour,
            availability_minute=minute,
        )
        scheduler = EtfFlowScheduler(
            ingestion_service=service,
            timezone_name=timezone_name,
            hour=hour,
            minute=minute,
            retry_until_success=retry_until_success,
            task_retry_base_seconds=task_retry_base_seconds,
            task_retry_max_seconds=task_retry_max_seconds,
        )
        if run_once:
            result = scheduler.run_once()
            typer.echo(json.dumps(result, ensure_ascii=False, indent=2, default=str))
            if result.get("retryable", False):
                raise typer.Exit(code=1)
            return
        scheduler.start()
    except (AkShareDataError, AkShareUnavailableError, ValueError) as exc:
        typer.echo(f"ETF 资金流调度失败：{exc}", err=True)
        raise typer.Exit(code=2) from exc


def _build_research_config(
    *,
    strategy: str,
    symbol: str,
    exchange: str | None,
    interval: str,
    start: str,
    end: str,
    mode: str,
    parameters: dict[str, Any] | None = None,
    parameter_space: dict[str, list[Any]] | None = None,
    train_window: int | None = None,
    test_window: int | None = None,
    auxiliary_intervals: list[str] | None = None,
    optimize_target: str = "sharpe",
) -> ResearchJobConfig:
    return ResearchJobConfig(
        strategy_name=strategy,
        symbol=symbol,
        exchange=exchange,
        interval=interval,
        start=datetime.fromisoformat(start),
        end=datetime.fromisoformat(end),
        mode=mode,
        parameters=parameters or {},
        parameter_space=parameter_space or {},
        train_window=train_window,
        test_window=test_window,
        auxiliary_intervals=auxiliary_intervals or [],
        optimize_target=optimize_target,
    )


@app.command("backtest")
def backtest_command(
    strategy: str = typer.Option(...),
    symbol: str = typer.Option(...),
    exchange: str | None = typer.Option(None, help="数据源交易所，例如 TRADINGVIEW 或 OKX"),
    interval: str = typer.Option(...),
    start: str = typer.Option(..., "--from"),
    end: str = typer.Option(..., "--to"),
    auxiliary_interval: list[str] = typer.Option([], "--aux-interval"),
) -> None:
    settings = Settings.from_env()
    settings.validate_for("backtest")
    context = create_command_context(settings)
    report = context.backtest(
        _build_research_config(
            strategy=strategy,
            symbol=symbol,
            exchange=exchange,
            interval=interval,
            start=start,
            end=end,
            mode="backtest",
            parameters=context.research_service.default_parameters(strategy),
            auxiliary_intervals=auxiliary_interval,
        )
    )
    typer.echo(report.to_markdown())


@app.command("optimize")
def optimize_command(
    strategy: str = typer.Option(...),
    symbol: str = typer.Option(...),
    exchange: str | None = typer.Option(None, help="数据源交易所，例如 TRADINGVIEW 或 OKX"),
    interval: str = typer.Option(...),
    start: str = typer.Option(..., "--from"),
    end: str = typer.Option(..., "--to"),
    method: str = typer.Option("ga"),
    auxiliary_interval: list[str] = typer.Option([], "--aux-interval"),
) -> None:
    settings = Settings.from_env()
    settings.validate_for("optimize")
    context = create_command_context(settings)
    report = context.optimize(
        _build_research_config(
            strategy=strategy,
            symbol=symbol,
            exchange=exchange,
            interval=interval,
            start=start,
            end=end,
            mode="optimize",
            parameter_space=context.research_service.default_parameter_space(strategy),
            auxiliary_intervals=auxiliary_interval,
        ),
        method=method,
    )
    typer.echo(report.to_markdown())


@app.command("walk-forward")
def walk_forward_command(
    strategy: str = typer.Option(...),
    symbol: str = typer.Option(...),
    exchange: str | None = typer.Option(None, help="数据源交易所，例如 TRADINGVIEW 或 OKX"),
    interval: str = typer.Option(...),
    start: str = typer.Option(..., "--from"),
    end: str = typer.Option(..., "--to"),
    train_window: int = typer.Option(...),
    test_window: int = typer.Option(...),
    auxiliary_interval: list[str] = typer.Option([], "--aux-interval"),
) -> None:
    settings = Settings.from_env()
    settings.validate_for("walk-forward")
    context = create_command_context(settings)
    report = context.walk_forward(
        _build_research_config(
            strategy=strategy,
            symbol=symbol,
            exchange=exchange,
            interval=interval,
            start=start,
            end=end,
            mode="walk-forward",
            parameter_space=context.research_service.default_parameter_space(strategy),
            train_window=train_window,
            test_window=test_window,
            auxiliary_intervals=auxiliary_interval,
        )
    )
    typer.echo(report.to_markdown())


@app.command("ablate-strategy")
def ablate_strategy_command(
    strategy: str = typer.Option(...),
    symbol: str = typer.Option(...),
    interval: str = typer.Option(...),
    start: str = typer.Option(..., "--from"),
    end: str = typer.Option(..., "--to"),
    variant: list[str] = typer.Option(..., help='Repeat NAME={"parameter": value}; each uses identical data.'),
    parameters_json: str = typer.Option("{}", help="Baseline parameter JSON"),
    exchange: str | None = typer.Option(None),
    auxiliary_interval: list[str] = typer.Option([], "--aux-interval"),
) -> None:
    """Run explicit, reproducible factor ablations without re-optimizing variants."""
    try:
        parameters = json.loads(parameters_json)
        variants = {
            item.split("=", 1)[0]: json.loads(item.split("=", 1)[1])
            for item in variant
            if "=" in item
        }
    except (json.JSONDecodeError, IndexError) as exc:
        raise typer.BadParameter("variants must be NAME={\"parameter\": value}") from exc
    if not variants or any(not name or not isinstance(values, dict) for name, values in variants.items()):
        raise typer.BadParameter("provide at least one valid NAME={...} variant")
    settings = Settings.from_env()
    settings.validate_for("backtest")
    result = create_command_context(settings).factor_ablation(
        _build_research_config(
            strategy=strategy, symbol=symbol, exchange=exchange, interval=interval,
            start=start, end=end, mode="backtest", parameters=parameters,
            auxiliary_intervals=auxiliary_interval,
        ),
        variants,
    )
    for row in result.variants:
        metrics = row["metrics"]
        typer.echo(
            f"{row['name']}: return={metrics.get('total_return', 0):.4%} "
            f"drawdown={metrics.get('max_drawdown', 0):.4%} trades={metrics.get('trade_count', 0):.0f}"
        )


@app.command("research-runs")
def research_runs_command(
    symbol: str | None = typer.Option(None),
    limit: int = typer.Option(20, min=1, max=100),
) -> None:
    """List persisted research evidence IDs used by version approval."""
    settings = Settings.from_env()
    settings.validate_for("backtest")
    rows = create_command_context(settings).research_run_repository.list_research_runs(
        symbol=symbol, limit=limit,
    )
    for run_id, report, status in rows:
        typer.echo(
            f"{run_id}\t{status}\t{report.mode}\t{report.strategy_name}\t"
            f"{report.symbol}\t{report.interval}\treturn={report.metrics.get('total_return', 0):.4%}\t"
            f"drawdown={report.metrics.get('max_drawdown', 0):.4%}"
        )


@app.command("validate-strategy")
def validate_strategy_command(
    strategy: str = typer.Option(...),
    symbol: str = typer.Option(...),
    interval: str = typer.Option(...),
    start: str = typer.Option(..., "--from"),
    end: str = typer.Option(..., "--to"),
    exchange: str | None = typer.Option(None),
    train_window: int = typer.Option(...),
    test_window: int = typer.Option(...),
    auxiliary_interval: list[str] = typer.Option([], "--aux-interval"),
    fixed_parameters_json: str = typer.Option("{}", help="固定在回测及每个走查折中的参数 JSON"),
) -> None:
    """Run backtest plus walk-forward gates used before version approval."""
    settings = Settings.from_env()
    settings.validate_for("walk-forward")
    context = create_command_context(settings)
    try:
        fixed_parameters = json.loads(fixed_parameters_json)
    except json.JSONDecodeError as exc:
        raise typer.BadParameter("--fixed-parameters-json must be a JSON object") from exc
    if not isinstance(fixed_parameters, dict):
        raise typer.BadParameter("--fixed-parameters-json must be a JSON object")
    common = {
        "strategy": strategy,
        "symbol": symbol,
        "exchange": exchange,
        "interval": interval,
        "start": start,
        "end": end,
        "auxiliary_intervals": auxiliary_interval,
    }
    result = context.validate_candidate(
        backtest_config=_build_research_config(
            **common,
            mode="backtest",
            parameters={**context.research_service.default_parameters(strategy), **fixed_parameters},
        ),
        walk_forward_config=_build_research_config(
            **common,
            mode="walk-forward",
            parameter_space={
                **context.research_service.default_parameter_space(strategy),
                **{name: [value] for name, value in fixed_parameters.items()},
            },
            train_window=train_window,
            test_window=test_window,
            # Approval is governed by sample-out-of-sample return, not the
            # in-sample Sharpe proxy used by exploratory research screens.
            optimize_target="return",
        ),
    )
    typer.echo(f"validation={'passed' if result.passed else 'failed'}")
    for reason in result.reasons:
        typer.echo(f"- {reason}")
    typer.echo(result.walk_forward.to_markdown())
    if not result.passed:
        raise typer.Exit(code=1)


def sync_target_market_data(context, sym, inv, logger) -> None:
    try:
        from datetime import datetime, timedelta, timezone
        now_dt = datetime.now(timezone.utc).replace(tzinfo=None)
        
        # Check database for latest bar time
        latest_bars = context.market_data_repository.fetch_latest_bars(sym, inv, limit=1)
        
        if latest_bars:
            latest_time = latest_bars[-1].datetime
            if latest_time.tzinfo is not None:
                latest_time = latest_time.replace(tzinfo=None)
            # If the latest bar in the DB is very fresh (e.g. less than 10 seconds old), skip sync
            if now_dt - latest_time < timedelta(seconds=10):
                logger.info(f"Data is already fresh for {sym} ({inv}), skipping sync.")
                return
            
            # OKX historical candles endpoint works best if start is slightly before the latest time to handle potential overlaps safely
            start_dt = latest_time - timedelta(minutes=5)
        else:
            inv_lower = inv.lower()
            if "m" in inv_lower:
                days = 3
            elif "h" in inv_lower:
                days = 25 if "4h" in inv_lower else 8
            elif "d" in inv_lower:
                days = 150
            else:
                days = 10
            start_dt = now_dt - timedelta(days=days)
            
        logger.info(f"Auto-syncing data for {sym} ({inv}) from {start_dt} to {now_dt} (incremental)")
        context.history_service.sync(
            symbol=sym,
            interval=inv,
            start=start_dt,
            end=now_dt,
            fill_missing=False,
        )
    except Exception as sync_err:
        logger.warning(f"Auto-sync failed for {sym} ({inv}): {sync_err}. Proceeding with local DB data.")


@app.command("live")
def live_command(
    once: bool = typer.Option(False, help="Run a single dependency probe and exit."),
    heartbeat_interval: int = typer.Option(30, min=5),
    strategy: str | None = typer.Option(None),
    symbol: str | None = typer.Option(None),
    interval: str | None = typer.Option(None),
    method: str = typer.Option("ga"),
) -> None:
    import logging
    logger = logging.getLogger(__name__)

    settings = Settings.from_env()
    settings.validate_for("live")

    # Apply persistent config overrides before initializing context
    from vntdr.services.config_service import ConfigService
    config_service = ConfigService(settings)

    context = create_command_context(settings)
    result = context.doctor()
    for line in result.lines():
        typer.echo(line)
    if not result.ok:
        raise typer.Exit(code=1)
    selected_strategy = strategy or settings.research.default_strategy
    selected_symbol = symbol or settings.research.default_symbol
    selected_interval = interval or settings.research.default_interval

    # Reconcile positions from OKX API for all monitored targets at startup
    config_service._load_overrides()
    targets = getattr(settings.research, "monitored_targets", None)
    if not targets:
        if strategy or symbol or interval:
            selected_strategy = strategy or settings.research.default_strategy
            selected_symbol = symbol or settings.research.default_symbol
            selected_interval = interval or settings.research.default_interval
            targets = [{
                "strategy_name": selected_strategy,
                "symbol": selected_symbol,
                "interval": selected_interval,
                "volume": settings.research.default_order_size
            }]
        else:
            targets = []

    if not targets:
        logger.warning("No monitored targets configured. Please add monitored targets through the Web UI or config override file.")

    for tgt in targets:
        s_name = tgt.get("strategy_name", strategy or settings.research.default_strategy)
        sym = tgt.get("symbol", symbol or settings.research.default_symbol)
        inv = tgt.get("interval", interval or settings.research.default_interval)
        
        cache_key = f"signal:{sym}:{inv}:{s_name}"
        # Get existing signal from Redis
        existing_signal = context.monitoring_service.signal_store.get(cache_key)
        
        if existing_signal is None:
            logger.info(f"No existing signal found in Redis for {sym} ({inv}), reconciling from OKX API positions")
            try:
                reconciled_signal = context.monitoring_service.reconcile_positions(symbol=sym)
                if reconciled_signal is not None:
                    context.monitoring_service.signal_store.set(cache_key, reconciled_signal)
                    logger.info(f"Reconciled signal {reconciled_signal} saved to cache for {sym}")
                else:
                    logger.info(f"No open positions found on OKX for {sym}, starting fresh")
            except Exception as e:
                logger.error(f"Failed to reconcile positions from OKX for {sym}: {e}, starting with empty position")
        else:
            logger.info(f"Found existing signal {existing_signal} in Redis for {sym} ({inv}), skipping reconciliation")

    # Create a local ThreadPoolExecutor for concurrent sync and monitoring of targets
    # Limit max workers to avoid excessive concurrent connection limits
    executor = ThreadPoolExecutor(max_workers=max(min(len(targets), 4), 1))

    def run_monitor_once() -> None:
        config_service._load_overrides()
        loop_targets = getattr(settings.research, "monitored_targets", None)
        if not loop_targets:
            if strategy or symbol or interval:
                selected_strategy = strategy or settings.research.default_strategy
                selected_symbol = symbol or settings.research.default_symbol
                selected_interval = interval or settings.research.default_interval
                loop_targets = [{
                    "strategy_name": selected_strategy,
                    "symbol": selected_symbol,
                    "interval": selected_interval,
                    "volume": settings.research.default_order_size
                }]
            else:
                loop_targets = []

        futures = []
        for tgt in loop_targets:
            s_name = tgt.get("strategy_name", strategy or settings.research.default_strategy)
            sym = tgt.get("symbol", symbol or settings.research.default_symbol)
            inv = tgt.get("interval", interval or settings.research.default_interval)
            vol = tgt.get("volume", settings.research.default_order_size)
            target_parameters = tgt.get("parameters")
            if not isinstance(target_parameters, dict) or not target_parameters:
                target_parameters = None

            def target_task(s_name=s_name, sym=sym, inv=inv, vol=vol, target_parameters=target_parameters):
                # 1. Incremental Sync
                sync_target_market_data(context, sym, inv, logger)
                # 2. Run Monitor
                return context.monitor_once(
                    strategy_name=s_name,
                    symbol=sym,
                    interval=inv,
                    method=method,
                    volume=vol,
                    parameters=target_parameters,
                )

            futures.append(executor.submit(target_task))

        for future in futures:
            try:
                res = future.result()
                typer.echo(
                    f"monitor strategy={res.strategy_name} symbol={res.symbol} "
                    f"interval={res.interval} signal={res.signal} "
                    f"actions={res.actions} parameters={res.best_parameters}"
                )
            except Exception as e:
                logger.error(f"Async monitor task failed: {e}")

    try:
        run_monitor_once()
    except Exception as e:
        logger.warning(f"Initial monitor run failed (no data yet?): {e}")
    if once:
        raise typer.Exit(code=0)
    
    # Start Telegram bot in background thread if token is configured
    if settings.telegram.bot_token and settings.telegram.chat_id:
        import threading

        from vntdr.adapters.telegram_bot import TelegramCommandBot
        from vntdr.services.config_service import ConfigService
        logger.info("Starting Telegram command bot in background thread")
        config_service = ConfigService(settings)
        redis_client = redis.from_url(settings.redis.url)
        bot = TelegramCommandBot(
            bot_token=settings.telegram.bot_token.get_secret_value(),
            chat_id=settings.telegram.chat_id,
            research_service=context.telegram_research(),
            monitor_once_callback=context.monitor_once,
            config_service=config_service,
            redis_client=redis_client,
        )
        # Start bot in a separate daemon thread
        thread = threading.Thread(target=bot.run, daemon=True)
        thread.start()
        logger.info("Telegram bot started in background")
    
    # Exponential backoff setup
    error_count = 0
    max_backoff = 300  # Maximum backoff in seconds (5 minutes)
    base_backoff = heartbeat_interval

    # If Telegram bot is active, the main loop should only act as a fallback
    # or skip monitoring to avoid duplicate notifications with the Bot's job queue.
    # We'll make it skip if it's already being handled by Telegram JobQueue conceptually,
    # or just let it run but ensure they use the same interval settings.
    
    while True:
        try:
            # Refresh settings/overrides each loop to pick up changes from /config
            config_service._load_overrides()
            current_strategy = config_service.get("research.default_strategy")
            current_symbol = config_service.get("research.default_symbol")
            current_interval = config_service.get("research.default_interval")
            if current_strategy != selected_strategy:
                logger.info(f"Main loop detected strategy change: {selected_strategy} -> {current_strategy}")
                selected_strategy = current_strategy
            if current_symbol != selected_symbol:
                logger.info(f"Main loop detected symbol change: {selected_symbol} -> {current_symbol}")
                selected_symbol = current_symbol
            if current_interval != selected_interval:
                logger.info(f"Main loop detected interval change: {selected_interval} -> {current_interval}")
                selected_interval = current_interval

            time.sleep(base_backoff if error_count == 0 else min(base_backoff * (2 ** error_count), max_backoff))
            run_monitor_once()
            # Reset error count on success
            if error_count > 0:
                logger.info("Monitoring recovered after errors, resetting backoff")
                error_count = 0
        except Exception as e:
            error_count += 1
            backoff = min(base_backoff * (2 ** error_count), max_backoff)
            logger.exception(f"Error in monitoring loop (error count: {error_count}), backing off for {backoff} seconds")
            if error_count >= 5:
                import html
                escaped_e = html.escape(str(e))
                context.monitoring_service.notifier.notify(
                    f"⚠️ Monitoring experiencing repeated errors: {escaped_e}\nBacking off for {backoff} seconds"
                )
            time.sleep(backoff)


@app.command("gradio")
def gradio_command(
    port: int | None = typer.Option(
        None,
        help="Port to listen on. Defaults to GRADIO_PORT or 7860.",
    ),
) -> None:
    from vntdr.webapp import main as run_webapp
    run_webapp(port=_resolve_gradio_port(port))


@app.command("telegram-bot")
def telegram_bot_command() -> None:
    import logging
    logger = logging.getLogger(__name__)
    
    settings = Settings.from_env()
    settings.validate_for("live")
    context = create_command_context(settings)
    from vntdr.adapters.telegram_bot import TelegramCommandBot
    from vntdr.services.config_service import ConfigService

    config_service = ConfigService(settings)
    redis_client = redis.from_url(settings.redis.url)
    bot = TelegramCommandBot(
        bot_token=settings.telegram.bot_token.get_secret_value() if settings.telegram.bot_token else "",
        chat_id=settings.telegram.chat_id or "",
        research_service=context.telegram_research(),
        monitor_once_callback=context.monitor_once,
        config_service=config_service,
        redis_client=redis_client,
    )
    
    bot.run()
