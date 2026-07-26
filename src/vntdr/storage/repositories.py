from __future__ import annotations

from datetime import datetime
from typing import Any, Sequence
import asyncio
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import select

from vntdr.models import (
    BarRecord, FactorObservation, FoldResult, Instrument, Interval, ResearchReport,
    ShadowRun, StrategyActivation, StrategyInstance, StrategyVersion,
)
from vntdr.storage.database import (
    BarORM, Database, FactorObservationORM, ResearchRunORM, StrategyActivationORM,
    ShadowRunORM, StrategyInstanceORM, StrategyVersionORM, SyncJobORM, WalkForwardFoldORM,
)


class MarketDataRepository:
    def __init__(self, database: Database) -> None:
        self.database = database
        self._executor = ThreadPoolExecutor(max_workers=4)

    def upsert_bars(self, bars: Sequence[BarRecord]) -> int:
        inserted = 0
        with self.database.session() as session:
            for bar in bars:
                existing = session.scalar(
                    select(BarORM).where(
                        BarORM.symbol == bar.symbol,
                        BarORM.exchange == bar.exchange,
                        BarORM.interval == bar.interval,
                        BarORM.datetime == bar.datetime,
                    )
                )
                if existing:
                    existing.open = bar.open
                    existing.high = bar.high
                    existing.low = bar.low
                    existing.close = bar.close
                    existing.volume = bar.volume
                    existing.is_synthetic = bar.is_synthetic
                    continue
                session.add(
                    BarORM(
                        symbol=bar.symbol,
                        exchange=bar.exchange,
                        interval=bar.interval,
                        datetime=bar.datetime,
                        open=bar.open,
                        high=bar.high,
                        low=bar.low,
                        close=bar.close,
                        volume=bar.volume,
                        is_synthetic=bar.is_synthetic,
                    )
                )
                inserted += 1
        return inserted

    async def upsert_bars_async(self, bars: Sequence[BarRecord]) -> int:
        return await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self.upsert_bars,
            bars
        )

    def upsert_bars_from_payloads(self, payloads: Sequence[dict[str, Any]]) -> int:
        bars = [BarRecord.model_validate(payload) for payload in payloads]
        return self.upsert_bars(bars)

    async def upsert_bars_from_payloads_async(self, payloads: Sequence[dict[str, Any]]) -> int:
        return await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self.upsert_bars_from_payloads,
            payloads
        )

    def fetch_bars(
        self,
        symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
        exchange: str | None = None,
    ) -> list[BarRecord]:
        # Handle interval case-insensitivity to prevent data loss due to OKX upper/lower case drift
        intervals = {interval.lower(), interval.upper(), interval}
        with self.database.session() as session:
            query = select(BarORM).where(
                BarORM.symbol == symbol,
                BarORM.interval.in_(intervals),
                BarORM.datetime >= start,
                BarORM.datetime <= end,
            )
            if exchange is not None:
                query = query.where(BarORM.exchange == exchange.upper())
            rows = session.scalars(query.order_by(BarORM.datetime.asc())).all()
        return [
            BarRecord(
                symbol=row.symbol,
                exchange=row.exchange,
                interval=row.interval,
                datetime=row.datetime,
                open=row.open,
                high=row.high,
                low=row.low,
                close=row.close,
                volume=row.volume,
                is_synthetic=row.is_synthetic,
            )
            for row in rows
        ]

    async def fetch_bars_async(
        self,
        symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
        exchange: str | None = None,
    ) -> list[BarRecord]:
        return await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self.fetch_bars,
            symbol,
            interval,
            start,
            end,
            exchange,
        )

    def fetch_latest_bars(
        self,
        symbol: str,
        interval: str,
        *,
        limit: int,
        exchange: str | None = None,
    ) -> list[BarRecord]:
        # Handle interval case-insensitivity to prevent data loss due to OKX upper/lower case drift
        intervals = {interval.lower(), interval.upper(), interval}
        with self.database.session() as session:
            query = select(BarORM).where(
                BarORM.symbol == symbol, BarORM.interval.in_(intervals)
            )
            if exchange is not None:
                query = query.where(BarORM.exchange == exchange.upper())
            rows = session.scalars(query.order_by(BarORM.datetime.desc()).limit(limit)).all()
        rows = list(reversed(rows))
        return [
            BarRecord(
                symbol=row.symbol,
                exchange=row.exchange,
                interval=row.interval,
                datetime=row.datetime,
                open=row.open,
                high=row.high,
                low=row.low,
                close=row.close,
                volume=row.volume,
                is_synthetic=row.is_synthetic,
            )
            for row in rows
        ]

    async def fetch_latest_bars_async(
        self,
        symbol: str,
        interval: str,
        *,
        limit: int,
        exchange: str | None = None,
    ) -> list[BarRecord]:
        from functools import partial

        return await asyncio.get_event_loop().run_in_executor(
            self._executor,
            partial(
                self.fetch_latest_bars,
                symbol,
                interval,
                limit=limit,
                exchange=exchange,
            ),
        )


class StrategyRepository:
    """Persistence boundary for versioned strategy configuration and factor data."""

    def __init__(self, database: Database) -> None:
        self.database = database

    def create_version(self, version: StrategyVersion) -> StrategyVersion:
        with self.database.session() as session:
            session.add(StrategyVersionORM(
                id=str(version.id), strategy_name=version.strategy_name,
                parameters=version.parameters, factor_config=version.factor_config,
                code_version=version.code_version, created_at=version.created_at,
                parent_id=str(version.parent_id) if version.parent_id else None,
            ))
        return version

    def create_instance(self, instance: StrategyInstance) -> StrategyInstance:
        with self.database.session() as session:
            session.add(StrategyInstanceORM(
                id=str(instance.id), name=instance.name, symbol=instance.instrument.symbol,
                exchange=instance.instrument.exchange, asset_class=instance.instrument.asset_class,
                calendar=instance.instrument.calendar, quote_currency=instance.instrument.quote_currency,
                primary_interval=instance.primary_interval.value,
                auxiliary_intervals=[item.value for item in instance.auxiliary_intervals],
                execution_mode=instance.execution_mode, enabled=instance.enabled,
            ))
        return instance

    def get_instance(self, instance_id: str) -> StrategyInstance | None:
        with self.database.session() as session:
            row = session.get(StrategyInstanceORM, str(instance_id))
        if row is None:
            return None
        return StrategyInstance(
            id=row.id,
            name=row.name,
            instrument=Instrument(
                symbol=row.symbol, exchange=row.exchange, asset_class=row.asset_class,
                calendar=row.calendar, quote_currency=row.quote_currency,
            ),
            primary_interval=Interval(value=row.primary_interval),
            auxiliary_intervals=[Interval(value=value) for value in row.auxiliary_intervals],
            execution_mode=row.execution_mode,
            enabled=row.enabled,
        )

    def list_instances(self, *, enabled_only: bool = False) -> list[StrategyInstance]:
        with self.database.session() as session:
            query = select(StrategyInstanceORM).order_by(StrategyInstanceORM.name)
            if enabled_only:
                query = query.where(StrategyInstanceORM.enabled.is_(True))
            identifiers = [row.id for row in session.scalars(query).all()]
        return [instance for identifier in identifiers if (instance := self.get_instance(identifier)) is not None]

    def activate(self, activation: StrategyActivation) -> StrategyActivation:
        with self.database.session() as session:
            if session.get(StrategyInstanceORM, str(activation.instance_id)) is None:
                raise ValueError(f"Unknown strategy instance: {activation.instance_id}")
            if session.get(StrategyVersionORM, str(activation.strategy_version_id)) is None:
                raise ValueError(f"Unknown strategy version: {activation.strategy_version_id}")
            session.add(StrategyActivationORM(
                instance_id=str(activation.instance_id), strategy_version_id=str(activation.strategy_version_id),
                effective_at=activation.effective_at, approved_by=activation.approved_by,
                rollback_of=str(activation.rollback_of) if activation.rollback_of else None,
            ))
        return activation

    def active_version(self, instance_id: str, at: datetime) -> StrategyVersion | None:
        with self.database.session() as session:
            activation = session.scalars(
                select(StrategyActivationORM)
                .where(StrategyActivationORM.instance_id == str(instance_id), StrategyActivationORM.effective_at <= at)
                .order_by(StrategyActivationORM.effective_at.desc(), StrategyActivationORM.id.desc())
            ).first()
            if activation is None:
                return None
            row = session.get(StrategyVersionORM, activation.strategy_version_id)
            if row is None:
                return None
            return StrategyVersion(
                id=row.id, strategy_name=row.strategy_name, parameters=row.parameters,
                factor_config=row.factor_config, code_version=row.code_version,
                created_at=row.created_at, parent_id=row.parent_id,
            )

    @staticmethod
    def _shadow_from_row(row: ShadowRunORM) -> ShadowRun:
        return ShadowRun(
            id=row.id, instance_id=row.instance_id, strategy_version_id=row.strategy_version_id,
            started_at=row.started_at, last_observed_at=row.last_observed_at,
            initial_equity=row.initial_equity, current_equity=row.current_equity,
            peak_equity=row.peak_equity, max_drawdown=row.max_drawdown,
            observation_count=row.observation_count, status=row.status,
        )

    def create_shadow_run(self, run: ShadowRun) -> ShadowRun:
        with self.database.session() as session:
            if session.get(StrategyInstanceORM, str(run.instance_id)) is None:
                raise ValueError(f"Unknown strategy instance: {run.instance_id}")
            if session.get(StrategyVersionORM, str(run.strategy_version_id)) is None:
                raise ValueError(f"Unknown strategy version: {run.strategy_version_id}")
            session.add(ShadowRunORM(
                id=str(run.id), instance_id=str(run.instance_id), strategy_version_id=str(run.strategy_version_id),
                started_at=run.started_at, last_observed_at=run.last_observed_at,
                initial_equity=run.initial_equity, current_equity=run.current_equity,
                peak_equity=run.peak_equity, max_drawdown=run.max_drawdown,
                observation_count=run.observation_count, status=run.status,
            ))
        return run

    def get_shadow_run(self, run_id: str) -> ShadowRun | None:
        with self.database.session() as session:
            row = session.get(ShadowRunORM, str(run_id))
            return self._shadow_from_row(row) if row is not None else None

    def list_shadow_runs(self, *, instance_id: str | None = None) -> list[ShadowRun]:
        with self.database.session() as session:
            query = select(ShadowRunORM).order_by(ShadowRunORM.started_at.desc())
            if instance_id is not None:
                query = query.where(ShadowRunORM.instance_id == str(instance_id))
            rows = session.scalars(query).all()
        return [self._shadow_from_row(row) for row in rows]

    def record_shadow_equity(self, run_id: str, equity: float, observed_at: datetime) -> ShadowRun:
        if equity <= 0:
            raise ValueError("shadow equity must be positive")
        with self.database.session() as session:
            row = session.get(ShadowRunORM, str(run_id))
            if row is None:
                raise ValueError(f"Unknown shadow run: {run_id}")
            if row.status != "active":
                raise ValueError("Cannot update a completed shadow run")
            row.current_equity = equity
            row.peak_equity = max(row.peak_equity, equity)
            row.max_drawdown = min(row.max_drawdown, equity / row.peak_equity - 1)
            row.last_observed_at = observed_at
            row.observation_count += 1
            session.flush()
            return self._shadow_from_row(row)

    def finalize_shadow_run(
        self,
        run_id: str,
        status: str,
        *,
        minimum_duration_days: int = 28,
        max_drawdown_limit: float = 0.10,
    ) -> ShadowRun:
        if status not in {"passed", "failed"}:
            raise ValueError("shadow status must be passed or failed")
        with self.database.session() as session:
            row = session.get(ShadowRunORM, str(run_id))
            if row is None:
                raise ValueError(f"Unknown shadow run: {run_id}")
            if status == "passed":
                if row.last_observed_at is None or row.observation_count == 0:
                    raise ValueError("A passed shadow run requires at least one equity observation")
                elapsed = row.last_observed_at - row.started_at
                if elapsed.total_seconds() < minimum_duration_days * 86400:
                    raise ValueError(f"A passed shadow run requires {minimum_duration_days} days of observation")
                if abs(row.max_drawdown) > max_drawdown_limit:
                    raise ValueError("A passed shadow run exceeds the drawdown limit")
            row.status = status
            session.flush()
            return self._shadow_from_row(row)

    def upsert_factor(self, observation: FactorObservation) -> None:
        with self.database.session() as session:
            row = session.scalar(select(FactorObservationORM).where(
                FactorObservationORM.symbol == observation.instrument.symbol,
                FactorObservationORM.exchange == observation.instrument.exchange,
                FactorObservationORM.factor_name == observation.factor_name,
                FactorObservationORM.observed_at == observation.observed_at,
                FactorObservationORM.interval == (observation.interval.value if observation.interval else None),
            ))
            values = dict(value=observation.value, available_at=observation.available_at, metadata_json=observation.metadata)
            if row is None:
                session.add(FactorObservationORM(
                    symbol=observation.instrument.symbol, exchange=observation.instrument.exchange,
                    factor_name=observation.factor_name, observed_at=observation.observed_at,
                    interval=observation.interval.value if observation.interval else None, **values,
                ))
            else:
                for key, value in values.items():
                    setattr(row, key, value)

    def factors_available_at(self, instrument: Instrument, at: datetime) -> list[FactorObservation]:
        """Return only observations that could have been known at *at* (no look-ahead)."""
        with self.database.session() as session:
            rows = session.scalars(
                select(FactorObservationORM).where(
                    FactorObservationORM.symbol == instrument.symbol,
                    FactorObservationORM.exchange == instrument.exchange,
                    FactorObservationORM.available_at <= at,
                    FactorObservationORM.observed_at <= at,
                ).order_by(FactorObservationORM.observed_at.asc())
            ).all()
        return [FactorObservation(
            instrument=instrument, factor_name=row.factor_name, value=row.value,
            observed_at=row.observed_at, available_at=row.available_at,
            interval=Interval(value=row.interval) if row.interval else None, metadata=row.metadata_json,
        ) for row in rows]


class ResearchRunRepository:
    def __init__(self, database: Database) -> None:
        self.database = database
        self._executor = ThreadPoolExecutor(max_workers=4)

    def create_sync_job(self, symbol: str, interval: str, start: datetime, end: datetime) -> int:
        with self.database.session() as session:
            job = SyncJobORM(symbol=symbol, interval=interval, start_at=start, end_at=end, status="started")
            session.add(job)
            session.flush()
            return int(job.id)

    async def create_sync_job_async(self, symbol: str, interval: str, start: datetime, end: datetime) -> int:
        return await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self.create_sync_job,
            symbol,
            interval,
            start,
            end
        )

    def complete_sync_job(
        self,
        job_id: int,
        *,
        status: str,
        inserted_count: int = 0,
        cleaned_count: int = 0,
        duplicates_removed: int = 0,
        error: str | None = None,
    ) -> None:
        with self.database.session() as session:
            job = session.get(SyncJobORM, job_id)
            if job is None:
                raise ValueError(f"Unknown sync job: {job_id}")
            job.status = status
            job.inserted_count = inserted_count
            job.cleaned_count = cleaned_count
            job.duplicates_removed = duplicates_removed
            job.error = error

    async def complete_sync_job_async(
        self,
        job_id: int,
        *,
        status: str,
        inserted_count: int = 0,
        cleaned_count: int = 0,
        duplicates_removed: int = 0,
        error: str | None = None,
    ) -> None:
        await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self.complete_sync_job,
            job_id,
            status,
            inserted_count,
            cleaned_count,
            duplicates_removed,
            error
        )

    def create_research_run(self, report: ResearchReport, config: dict[str, Any]) -> int:
        with self.database.session() as session:
            run = ResearchRunORM(
                mode=report.mode,
                strategy_name=report.strategy_name,
                symbol=report.symbol,
                interval=report.interval,
                status="started",
                config=config,
                metrics=report.metrics,
                best_parameters=report.best_parameters,
                top_results=report.top_results,
            )
            session.add(run)
            session.flush()
            return int(run.id)

    def get_research_run(self, run_id: int) -> tuple[ResearchReport, dict[str, Any], str] | None:
        with self.database.session() as session:
            row = session.get(ResearchRunORM, run_id)
            if row is None:
                return None
            folds = session.scalars(
                select(WalkForwardFoldORM)
                .where(WalkForwardFoldORM.research_run_id == run_id)
                .order_by(WalkForwardFoldORM.fold_index)
            ).all()
        return (
            ResearchReport(
                strategy_name=row.strategy_name, symbol=row.symbol, interval=row.interval,
                mode=row.mode, metrics=row.metrics, best_parameters=row.best_parameters,
                top_results=row.top_results,
                fold_results=[FoldResult(
                    fold_index=fold.fold_index, train_start=fold.train_start, train_end=fold.train_end,
                    test_start=fold.test_start, test_end=fold.test_end,
                    metrics=fold.metrics, parameters=fold.parameters,
                ) for fold in folds],
            ),
            row.config,
            row.status,
        )

    async def create_research_run_async(self, report: ResearchReport, config: dict[str, Any]) -> int:
        return await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self.create_research_run,
            report,
            config
        )

    def finalize_research_run(
        self,
        run_id: int,
        *,
        status: str,
        metrics: dict[str, Any],
        best_parameters: dict[str, Any],
        top_results: list[dict[str, Any]],
        report_path: str,
    ) -> None:
        with self.database.session() as session:
            run = session.get(ResearchRunORM, run_id)
            if run is None:
                raise ValueError(f"Unknown research run: {run_id}")
            run.status = status
            run.metrics = metrics
            run.best_parameters = best_parameters
            run.top_results = top_results
            run.report_path = report_path

    async def finalize_research_run_async(
        self,
        run_id: int,
        *,
        status: str,
        metrics: dict[str, Any],
        best_parameters: dict[str, Any],
        top_results: list[dict[str, Any]],
        report_path: str,
    ) -> None:
        await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self.finalize_research_run,
            run_id,
            status,
            metrics,
            best_parameters,
            top_results,
            report_path
        )

    def add_fold_result(self, run_id: int, fold: FoldResult) -> None:
        with self.database.session() as session:
            session.add(
                WalkForwardFoldORM(
                    research_run_id=run_id,
                    fold_index=fold.fold_index,
                    train_start=fold.train_start,
                    train_end=fold.train_end,
                    test_start=fold.test_start,
                    test_end=fold.test_end,
                    metrics=fold.metrics,
                    parameters=fold.parameters,
                )
            )

    async def add_fold_result_async(self, run_id: int, fold: FoldResult) -> None:
        await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self.add_fold_result,
            run_id,
            fold
        )
