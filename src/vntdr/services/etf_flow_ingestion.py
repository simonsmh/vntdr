"""Scheduled ingestion of an ETF money-flow universe."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

from vntdr.services.akshare_fund_flow import (
    AkShareDataError,
    AkShareFundFlowProvider,
    _market_for_code,
)
from vntdr.storage.repositories import EtfMoneyFlowRepository


class RetryableEtfFlowError(RuntimeError):
    """A completed ETF-flow task that should be scheduled again."""

    def __init__(self, result: dict[str, Any]) -> None:
        self.result = result
        super().__init__(
            f"ETF flow task is retryable: status={result.get('status')} "
            f"failed_count={result.get('failed_count')}"
        )


@dataclass(frozen=True)
class EtfWatchTarget:
    symbol: str
    market: str
    name: str | None = None

    def __post_init__(self) -> None:
        normalized_symbol = str(self.symbol).strip().zfill(6)
        normalized_market = str(self.market).strip().lower()
        if normalized_market not in {"sh", "sz", "bj"}:
            raise ValueError(f"unsupported ETF market: {self.market}")
        object.__setattr__(self, "symbol", normalized_symbol)
        object.__setattr__(self, "market", normalized_market)


DEFAULT_ETF_WATCHLIST: tuple[EtfWatchTarget, ...] = (
    EtfWatchTarget("588200", "sh", "嘉实上证科创板芯片ETF"),
    EtfWatchTarget("510300", "sh", "华泰柏瑞沪深300ETF"),
    EtfWatchTarget("588170", "sh", "华夏上证科创板半导体材料设备主题ETF"),
    EtfWatchTarget("588080", "sh", "易方达上证科创板50ETF"),
    EtfWatchTarget("159845", "sz", "华夏中证1000ETF"),
    EtfWatchTarget("159941", "sz", "广发纳指100ETF"),
    EtfWatchTarget("512400", "sh", "南方有色金属ETF"),
)


def parse_watchlist(raw: str | None) -> tuple[EtfWatchTarget, ...]:
    """Parse ``588200:sh,510300:sh,159845:sz`` into unique targets."""
    if not raw or not raw.strip():
        return DEFAULT_ETF_WATCHLIST
    targets: list[EtfWatchTarget] = []
    seen: set[str] = set()
    for token in raw.split(","):
        item = token.strip()
        if not item:
            continue
        parts = [part.strip() for part in item.split(":", 1)]
        symbol = parts[0].zfill(6)
        market = parts[1].lower() if len(parts) == 2 and parts[1] else _market_for_code(symbol)
        target = EtfWatchTarget(symbol, market)
        if target.symbol not in seen:
            targets.append(target)
            seen.add(target.symbol)
    if not targets:
        raise ValueError("ETF watchlist cannot be empty")
    return tuple(targets)


class EtfFlowIngestionService:
    """Fetch the watchlist and persist each normalized daily observation."""

    def __init__(
        self,
        *,
        provider: AkShareFundFlowProvider,
        repository: EtfMoneyFlowRepository,
        watchlist: tuple[EtfWatchTarget, ...] = DEFAULT_ETF_WATCHLIST,
        lookback_days: int = 120,
        timezone_name: str = "Asia/Shanghai",
        availability_hour: int = 16,
        availability_minute: int = 10,
        clock: Callable[[], datetime] | None = None,
        watchlist_resolver: Callable[[], tuple[EtfWatchTarget, ...]] | None = None,
        universe_label: str = "watchlist",
    ) -> None:
        if lookback_days < 1:
            raise ValueError("lookback_days must be >= 1")
        if not 0 <= availability_hour <= 23:
            raise ValueError("availability_hour must be between 0 and 23")
        if not 0 <= availability_minute <= 59:
            raise ValueError("availability_minute must be between 0 and 59")
        self.provider = provider
        self.repository = repository
        self.watchlist = tuple(watchlist)
        if not self.watchlist:
            raise ValueError("ETF watchlist cannot be empty")
        self.lookback_days = lookback_days
        self.timezone_name = timezone_name
        self._zone = ZoneInfo(timezone_name)
        self.availability_hour = availability_hour
        self.availability_minute = availability_minute
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._watchlist_resolver = watchlist_resolver
        self.universe_label = universe_label

    def _now(self) -> datetime:
        value = self._clock()
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)

    def _local_date(self, now: datetime) -> date:
        return now.astimezone(self._zone).date()

    def _availability_at(self, trade_date: date) -> datetime:
        local_close = datetime.combine(
            trade_date,
            time(self.availability_hour, self.availability_minute),
            tzinfo=self._zone,
        )
        return local_close.astimezone(timezone.utc)

    def run(
        self,
        *,
        end_date: date | None = None,
        start_date: date | None = None,
        run_key: str | None = None,
    ) -> dict[str, Any]:
        started_at = self._now()
        effective_end = end_date or self._local_date(started_at)
        effective_start = start_date or (effective_end - timedelta(days=self.lookback_days))
        if effective_start > effective_end:
            raise ValueError("start_date cannot be later than end_date")

        watchlist = self.watchlist
        if self._watchlist_resolver is not None:
            watchlist = tuple(self._watchlist_resolver())
            if not watchlist:
                raise AkShareDataError("ETF universe resolver returned no symbols")

        key = run_key or (
            f"etf-flow-{started_at.astimezone(self._zone):%Y%m%d-%H%M%S}-"
            f"{uuid4().hex[:8]}"
        )
        run_id = self.repository.create_run(
            run_key=key,
            started_at=started_at,
            requested_count=len(watchlist),
        )
        failures: list[dict[str, str]] = []
        successful_count = 0
        rows_seen = 0
        rows_inserted = 0
        retry_count = 0
        for target in watchlist:
            target_retry_count = 0
            try:
                frame = self.provider.fetch_symbol_frame(
                    symbol=target.symbol,
                    market=target.market,
                )
                target_retry_count = self.provider.retry_count
                frame = frame[
                    (frame["trade_date"] >= effective_start)
                    & (frame["trade_date"] <= effective_end)
                ]
                if frame.empty:
                    raise AkShareDataError(
                        f"no flow rows for {target.symbol} between "
                        f"{effective_start} and {effective_end}"
                    )
                frame = frame.copy()
                frame["available_at"] = frame["trade_date"].map(self._availability_at)
                rows_seen += len(frame)
                rows_inserted += self.repository.upsert_daily(
                    frame,
                    market=target.market,
                    available_at=started_at,
                    fetched_at=self._now(),
                    retry_count=target_retry_count,
                )
                successful_count += 1
            except Exception as exc:  # noqa: BLE001 - one failed ETF must not stop the batch
                target_retry_count = self.provider.retry_count
                failures.append({"symbol": target.symbol, "error": str(exc)})
            retry_count += target_retry_count

        failed_count = len(failures)
        status = "success" if failed_count == 0 else "partial" if successful_count else "failed"
        task_status = "succeeded" if failed_count == 0 else "retryable"
        finished_at = self._now()
        details = {
            "date_start": str(effective_start),
            "date_end": str(effective_end),
            "outcome": status,
            "task_status": task_status,
            "universe": self.universe_label,
            "universe_count": len(watchlist),
            "universe_symbols": [
                {"symbol": target.symbol, "name": target.name}
                for target in watchlist
            ],
            "rows_seen": rows_seen,
            "rows_inserted": rows_inserted,
            "failures": failures,
            "source": "akshare",
        }
        self.repository.complete_run(
            run_id,
            status="success" if failed_count == 0 else "retryable",
            finished_at=finished_at,
            successful_count=successful_count,
            failed_count=failed_count,
            retry_count=retry_count,
            details=details,
        )
        return {
            "run_id": run_id,
            "run_key": key,
            "status": status,
            "task_status": task_status,
            "retryable": failed_count > 0,
            "date_start": str(effective_start),
            "date_end": str(effective_end),
            "universe": self.universe_label,
            "universe_count": len(watchlist),
            "requested_count": len(watchlist),
            "successful_count": successful_count,
            "failed_count": failed_count,
            "rows_seen": rows_seen,
            "rows_inserted": rows_inserted,
            "retry_count": retry_count,
            "failures": failures,
            "source": "akshare",
        }
