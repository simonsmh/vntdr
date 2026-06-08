from __future__ import annotations

import importlib
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor
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
        from vntdr.services.config_service import ConfigService
        ConfigService(self.settings)._load_overrides()
        return self.monitoring_service.monitor_once(
            strategy_name=strategy_name,
            symbol=symbol,
            interval=interval,
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
        parameter_space: dict[str, list[Any]] | None = None,
    ) -> MonitorResult:
        from vntdr.services.config_service import ConfigService
        ConfigService(self.settings)._load_overrides()
        return await self.monitoring_service.monitor_once_async(
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
    method: str = typer.Option("ga"),
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

            def target_task(s_name=s_name, sym=sym, inv=inv, vol=vol):
                # 1. Incremental Sync
                sync_target_market_data(context, sym, inv, logger)
                # 2. Run Monitor
                return context.monitor_once(
                    strategy_name=s_name,
                    symbol=sym,
                    interval=inv,
                    method=method,
                    volume=vol,
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
    port: int = typer.Option(7860, help="Port to listen on"),
) -> None:
    from vntdr.webapp import main as run_webapp
    run_webapp(port=port)


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
