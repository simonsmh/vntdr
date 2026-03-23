from __future__ import annotations

import importlib
import time
from datetime import datetime
from typing import Any

import redis
import typer

from vntdr.config import Settings
from vntdr.adapters.orders import OkxOrderExecutor, SimulatedOrderExecutor
from vntdr.adapters.state import RedisSignalStore
from vntdr.adapters.telegram import TelegramNotifier
from vntdr.models import HealthCheckResult, MonitorResult, ResearchJobConfig, SyncResult
from vntdr.services.history import HistorySyncService, OkxHistoryClient
from vntdr.services.monitoring import MonitoringService
from vntdr.services.research import ResearchService
from vntdr.services.risk import RiskManager
from vntdr.services.telegram_research import TelegramResearchService
from vntdr.storage.database import Database
from vntdr.storage.repositories import MarketDataRepository, ResearchRunRepository

app = typer.Typer(add_completion=False, no_args_is_help=True)


class CommandContext:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.database = Database(settings.database.dsn)
        self.database.create_schema()
        self.market_data_repository = MarketDataRepository(self.database)
        self.research_run_repository = ResearchRunRepository(self.database)
        self.history_service = HistorySyncService(
            settings=settings,
            history_client=OkxHistoryClient(
                base_url=settings.okx.rest_base_url,
                demo_trading=settings.okx.demo_trading,
            ),
            market_data_repository=self.market_data_repository,
            research_run_repository=self.research_run_repository,
        )
        self.research_service = ResearchService(
            settings=settings,
            market_data_repository=self.market_data_repository,
            research_run_repository=self.research_run_repository,
        )
        redis_client = redis.from_url(settings.redis.url)
        self.monitoring_service = MonitoringService(
            research_service=self.research_service,
            market_data_repository=self.market_data_repository,
            notifier=TelegramNotifier(
                bot_token=settings.telegram.bot_token.get_secret_value() if settings.telegram.bot_token else "",
                chat_id=settings.telegram.chat_id or "",
            ),
            order_executor=self._build_order_executor(settings),
            signal_store=RedisSignalStore(redis_client),
            risk_manager=RiskManager(settings.risk),
        )
        self.telegram_research_service = TelegramResearchService(
            settings=settings,
            history_service=self.history_service,
            research_service=self.research_service,
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

    def telegram_research(self) -> TelegramResearchService:
        return self.telegram_research_service

    def monitor_once(
        self,
        *,
        strategy_name: str,
        symbol: str,
        interval: str,
        method: str,
        volume: float,
        parameter_space: dict[str, list[Any]] | None = None,
    ) -> MonitorResult:
        if parameter_space is None:
            parameter_space = self.research_service.default_parameter_space(strategy_name)
        return self.monitoring_service.monitor_once(
            strategy_name=strategy_name,
            symbol=symbol,
            interval=interval,
            parameter_space=parameter_space,
            volume=volume,
            method=method,
            lookback_bars=self.settings.research.monitor_lookback_bars,
        )


def create_command_context(settings: Settings) -> CommandContext:
    return CommandContext(settings)


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
    )
    typer.echo(
        f"sync job={result.job_id} inserted={result.inserted_count} cleaned={result.cleaned_count} "
        f"duplicates={result.duplicates_removed}"
    )


def _build_research_config(
    *,
    strategy: str,
    symbol: str,
    interval: str,
    start: str,
    end: str,
    mode: str,
    parameters: dict[str, Any] | None = None,
    parameter_space: dict[str, list[Any]] | None = None,
    train_window: int | None = None,
    test_window: int | None = None,
) -> ResearchJobConfig:
    return ResearchJobConfig(
        strategy_name=strategy,
        symbol=symbol,
        interval=interval,
        start=datetime.fromisoformat(start),
        end=datetime.fromisoformat(end),
        mode=mode,
        parameters=parameters or {},
        parameter_space=parameter_space or {},
        train_window=train_window,
        test_window=test_window,
    )


@app.command("backtest")
def backtest_command(
    strategy: str = typer.Option(...),
    symbol: str = typer.Option(...),
    interval: str = typer.Option(...),
    start: str = typer.Option(..., "--from"),
    end: str = typer.Option(..., "--to"),
    lookback: int = typer.Option(3),
) -> None:
    settings = Settings.from_env()
    settings.validate_for("backtest")
    context = create_command_context(settings)
    report = context.backtest(
        _build_research_config(
            strategy=strategy,
            symbol=symbol,
            interval=interval,
            start=start,
            end=end,
            mode="backtest",
            parameters={"lookback": lookback},
        )
    )
    typer.echo(report.to_markdown())


@app.command("optimize")
def optimize_command(
    strategy: str = typer.Option(...),
    symbol: str = typer.Option(...),
    interval: str = typer.Option(...),
    start: str = typer.Option(..., "--from"),
    end: str = typer.Option(..., "--to"),
    method: str = typer.Option("grid"),
    lookback_values: str = typer.Option("2,3,4"),
) -> None:
    settings = Settings.from_env()
    settings.validate_for("optimize")
    context = create_command_context(settings)
    values = [int(value.strip()) for value in lookback_values.split(",") if value.strip()]
    report = context.optimize(
        _build_research_config(
            strategy=strategy,
            symbol=symbol,
            interval=interval,
            start=start,
            end=end,
            mode="optimize",
            parameter_space={"lookback": values},
        ),
        method=method,
    )
    typer.echo(report.to_markdown())


@app.command("walk-forward")
def walk_forward_command(
    strategy: str = typer.Option(...),
    symbol: str = typer.Option(...),
    interval: str = typer.Option(...),
    start: str = typer.Option(..., "--from"),
    end: str = typer.Option(..., "--to"),
    train_window: int = typer.Option(...),
    test_window: int = typer.Option(...),
    lookback_values: str = typer.Option("2,3,4"),
) -> None:
    settings = Settings.from_env()
    settings.validate_for("walk-forward")
    context = create_command_context(settings)
    values = [int(value.strip()) for value in lookback_values.split(",") if value.strip()]
    report = context.walk_forward(
        _build_research_config(
            strategy=strategy,
            symbol=symbol,
            interval=interval,
            start=start,
            end=end,
            mode="walk-forward",
            parameter_space={"lookback": values},
            train_window=train_window,
            test_window=test_window,
        )
    )
    typer.echo(report.to_markdown())


@app.command("live")
def live_command(
    once: bool = typer.Option(False, help="Run a single dependency probe and exit."),
    heartbeat_interval: int = typer.Option(30, min=5),
    strategy: str | None = typer.Option(None),
    symbol: str | None = typer.Option(None),
    interval: str | None = typer.Option(None),
    method: str = typer.Option("grid"),
) -> None:
    settings = Settings.from_env()
    settings.validate_for("live")
    context = create_command_context(settings)
    result = context.doctor()
    for line in result.lines():
        typer.echo(line)
    if not result.ok:
        raise typer.Exit(code=1)
    selected_strategy = strategy or settings.research.default_strategy
    selected_symbol = symbol or settings.research.default_symbol
    selected_interval = interval or settings.research.default_interval

    def run_monitor_once() -> None:
        monitor_result = context.monitor_once(
            strategy_name=selected_strategy,
            symbol=selected_symbol,
            interval=selected_interval,
            method=method,
            volume=settings.research.default_order_size,
        )
        typer.echo(
            f"monitor strategy={monitor_result.strategy_name} symbol={monitor_result.symbol} "
            f"interval={monitor_result.interval} signal={monitor_result.signal} "
            f"actions={monitor_result.actions} parameters={monitor_result.best_parameters}"
        )

    run_monitor_once()
    if once:
        raise typer.Exit(code=0)
    while True:
        time.sleep(heartbeat_interval)
        run_monitor_once()


@app.command("telegram-bot")
def telegram_bot_command() -> None:
    settings = Settings.from_env()
    settings.validate_for("live")
    context = create_command_context(settings)
    from vntdr.adapters.telegram_bot import TelegramCommandBot

    bot = TelegramCommandBot(
        bot_token=settings.telegram.bot_token.get_secret_value() if settings.telegram.bot_token else "",
        chat_id=settings.telegram.chat_id or "",
        research_service=context.telegram_research(),
        monitor_once_callback=context.monitor_once,
    )
    bot.run()
