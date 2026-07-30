"""APScheduler wrapper for the daily ETF money-flow ingestion."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from vntdr.services.etf_flow_ingestion import (
    EtfFlowIngestionService,
    RetryableEtfFlowError,
)

LOGGER = logging.getLogger(__name__)


class EtfFlowScheduler:
    RETRY_JOB_ID = "etf-money-flow-retry"

    def __init__(
        self,
        *,
        ingestion_service: EtfFlowIngestionService,
        timezone_name: str = "Asia/Shanghai",
        hour: int = 16,
        minute: int = 10,
        retry_until_success: bool = True,
        task_retry_base_seconds: float = 60.0,
        task_retry_max_seconds: float = 1800.0,
    ) -> None:
        if not 0 <= hour <= 23:
            raise ValueError("hour must be between 0 and 23")
        if not 0 <= minute <= 59:
            raise ValueError("minute must be between 0 and 59")
        if task_retry_base_seconds <= 0:
            raise ValueError("task_retry_base_seconds must be > 0")
        if task_retry_max_seconds < task_retry_base_seconds:
            raise ValueError(
                "task_retry_max_seconds must be >= task_retry_base_seconds"
            )
        self.ingestion_service = ingestion_service
        self.timezone_name = timezone_name
        self.hour = hour
        self.minute = minute
        self.retry_until_success = retry_until_success
        self.task_retry_base_seconds = task_retry_base_seconds
        self.task_retry_max_seconds = task_retry_max_seconds
        self._task_retry_attempt = 0
        self._zone = ZoneInfo(timezone_name)
        self.scheduler = BlockingScheduler(timezone=ZoneInfo(timezone_name))

    def run_once(self) -> dict[str, Any]:
        result = self.ingestion_service.run()
        LOGGER.info("ETF money-flow ingestion completed: %s", result)
        return result

    def _task_retry_delay(self) -> float:
        delay = self.task_retry_base_seconds * (2 ** max(self._task_retry_attempt - 1, 0))
        return min(delay, self.task_retry_max_seconds)

    def _schedule_retry(self) -> None:
        if not self.retry_until_success:
            return
        self._task_retry_attempt += 1
        delay = self._task_retry_delay()
        next_run = datetime.now(self._zone) + timedelta(seconds=delay)
        if self.scheduler.get_job(self.RETRY_JOB_ID) is not None:
            LOGGER.info("ETF flow retry already scheduled; keeping the earlier retry")
            return
        self.scheduler.add_job(
            self._scheduled_run,
            trigger="date",
            run_date=next_run,
            id=self.RETRY_JOB_ID,
            replace_existing=True,
            max_instances=1,
            misfire_grace_time=3600,
        )
        LOGGER.warning(
            "ETF flow task marked retryable; scheduling attempt=%d in %.0fs",
            self._task_retry_attempt,
            delay,
        )

    def _scheduled_run(self) -> dict[str, Any]:
        try:
            result = self.ingestion_service.run()
        except Exception as exc:  # noqa: BLE001 - scheduler must keep retrying task failures
            result = {
                "status": "failed",
                "task_status": "retryable",
                "retryable": True,
                "error": str(exc),
            }
            self._schedule_retry()
            raise RetryableEtfFlowError(result) from exc
        if result.get("retryable", False):
            self._schedule_retry()
            raise RetryableEtfFlowError(result)
        self._task_retry_attempt = 0
        LOGGER.info("ETF flow task succeeded; task retry state reset")
        return result

    def start(self) -> None:
        self.scheduler.add_job(
            self._scheduled_run,
            trigger=CronTrigger(
                day_of_week="mon-fri",
                hour=self.hour,
                minute=self.minute,
                timezone=ZoneInfo(self.timezone_name),
            ),
            id="etf-money-flow-daily",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=3600,
        )
        LOGGER.info(
            "ETF money-flow scheduler started: weekdays at %02d:%02d %s",
            self.hour,
            self.minute,
            self.timezone_name,
        )
        self.scheduler.start()

    def shutdown(self) -> None:
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
