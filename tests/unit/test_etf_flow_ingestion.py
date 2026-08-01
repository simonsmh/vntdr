from __future__ import annotations

from datetime import date, datetime, timezone

import pandas as pd
import pytest

from vntdr.services.etf_flow_ingestion import (
    EtfFlowIngestionService,
    EtfWatchTarget,
    RetryableEtfFlowError,
    parse_watchlist,
)
from vntdr.services.etf_flow_scheduler import EtfFlowScheduler
from vntdr.storage.database import Database
from vntdr.storage.repositories import EtfMoneyFlowRepository


def _flow_frame(symbol: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": date(2026, 7, 29),
                "symbol": symbol,
                "main_net_inflow": 100.0,
                "main_inflow_ratio": 0.1,
                "extra_large_net_inflow": 60.0,
                "large_net_inflow": 40.0,
                "large_inflow_ratio": 0.04,
                "calculated_main_net_inflow": 100.0,
                "main_component_gap": 0.0,
                "close_price": 1.0,
                "pct_change": 0.5,
            }
        ]
    )


class FakeProvider:
    retry_count = 1

    def fetch_symbol_frame(self, *, symbol: str, market: str) -> pd.DataFrame:
        assert market == "sh"
        return _flow_frame(symbol)


def test_parse_watchlist_uses_code_market_and_deduplicates() -> None:
    targets = parse_watchlist("588200:sh,159845:sz,588200:sh")

    assert targets == (EtfWatchTarget("588200", "sh"), EtfWatchTarget("159845", "sz"))


def test_ingestion_upserts_rows_and_records_retry_count(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'etf-flow.db'}")
    database.create_schema()
    service = EtfFlowIngestionService(
        provider=FakeProvider(),
        repository=EtfMoneyFlowRepository(database),
        watchlist=(EtfWatchTarget("588200", "sh"),),
        lookback_days=10,
        clock=lambda: datetime(2026, 7, 30, 8, tzinfo=timezone.utc),
    )

    first = service.run(
        start_date=date(2026, 7, 29),
        end_date=date(2026, 7, 30),
        run_key="test-run-1",
    )
    second = service.run(
        start_date=date(2026, 7, 29),
        end_date=date(2026, 7, 30),
        run_key="test-run-2",
    )

    assert first["status"] == "success"
    assert first["retry_count"] == 1
    assert first["rows_seen"] == 1
    assert first["rows_inserted"] == 1
    assert second["status"] == "success"
    assert second["rows_seen"] == 1
    assert second["rows_inserted"] == 0
    assert EtfMoneyFlowRepository(database).count_daily(symbol="588200") == 1

    repository = EtfMoneyFlowRepository(database)
    rows = repository.fetch_daily(symbols=["588200"], start_date=date(2026, 7, 29))
    runs = repository.fetch_latest_runs(limit=2)
    assert rows[0]["symbol"] == "588200"
    assert rows[0]["trade_date"] == date(2026, 7, 29)
    assert len(runs) == 2
    assert runs[0]["status"] == "success"


def test_ingestion_resolves_dynamic_universe_before_each_run(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'dynamic-etf-flow.db'}")
    database.create_schema()

    class ResolvingProvider(FakeProvider):
        def fetch_etf_universe(self, **_: object) -> pd.DataFrame:
            return pd.DataFrame(
                [{"symbol": "588200", "name": "科创芯片ETF", "market": "sh", "total_market_cap": 1e10}]
            )

    provider = ResolvingProvider()
    service = EtfFlowIngestionService(
        provider=provider,
        repository=EtfMoneyFlowRepository(database),
        watchlist=(EtfWatchTarget("510300", "sh"),),
        watchlist_resolver=lambda: (EtfWatchTarget("588200", "sh"),),
        universe_label="total_market_cap>=10000000000",
        clock=lambda: datetime(2026, 7, 30, 8, tzinfo=timezone.utc),
    )

    result = service.run(
        start_date=date(2026, 7, 29),
        end_date=date(2026, 7, 30),
        run_key="dynamic-run-1",
    )

    assert result["requested_count"] == 1
    assert result["universe"] == "total_market_cap>=10000000000"
    assert EtfMoneyFlowRepository(database).count_daily(symbol="588200") == 1


def test_scheduler_marks_failed_task_retryable_and_queues_delayed_retry() -> None:
    class FailingService:
        def run(self) -> dict[str, object]:
            return {
                "status": "failed",
                "task_status": "retryable",
                "retryable": True,
                "failed_count": 1,
            }

    scheduler = EtfFlowScheduler(
        ingestion_service=FailingService(),
        task_retry_base_seconds=60,
        task_retry_max_seconds=120,
    )

    with pytest.raises(RetryableEtfFlowError):
        scheduler._scheduled_run()

    assert scheduler._task_retry_attempt == 1
    assert scheduler.scheduler.get_job(scheduler.RETRY_JOB_ID) is not None
