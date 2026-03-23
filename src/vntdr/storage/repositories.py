from __future__ import annotations

from datetime import datetime
from typing import Any, Sequence

from sqlalchemy import select

from vntdr.models import BarRecord, FoldResult, ResearchReport
from vntdr.storage.database import BarORM, Database, ResearchRunORM, SyncJobORM, WalkForwardFoldORM


class MarketDataRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

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

    def upsert_bars_from_payloads(self, payloads: Sequence[dict[str, Any]]) -> int:
        bars = [BarRecord.model_validate(payload) for payload in payloads]
        return self.upsert_bars(bars)

    def fetch_bars(
        self,
        symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
    ) -> list[BarRecord]:
        with self.database.session() as session:
            rows = session.scalars(
                select(BarORM)
                .where(
                    BarORM.symbol == symbol,
                    BarORM.interval == interval,
                    BarORM.datetime >= start,
                    BarORM.datetime <= end,
                )
                .order_by(BarORM.datetime.asc())
            ).all()
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

    def fetch_latest_bars(
        self,
        symbol: str,
        interval: str,
        *,
        limit: int,
    ) -> list[BarRecord]:
        with self.database.session() as session:
            rows = session.scalars(
                select(BarORM)
                .where(BarORM.symbol == symbol, BarORM.interval == interval)
                .order_by(BarORM.datetime.desc())
                .limit(limit)
            ).all()
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


class ResearchRunRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    def create_sync_job(self, symbol: str, interval: str, start: datetime, end: datetime) -> int:
        with self.database.session() as session:
            job = SyncJobORM(symbol=symbol, interval=interval, start_at=start, end_at=end, status="started")
            session.add(job)
            session.flush()
            return int(job.id)

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
