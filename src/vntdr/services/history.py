from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Protocol

import okx.MarketData as MarketData
from tenacity import Retrying, stop_after_attempt, wait_fixed

from vntdr.cleaning import clean_bars
from vntdr.config import Settings
from vntdr.models import SyncResult
from vntdr.storage.repositories import MarketDataRepository, ResearchRunRepository


class HistoryClient(Protocol):
    def fetch_candles(
        self,
        symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
        limit: int,
    ) -> list[dict[str, Any]]:
        ...


class OkxHistoryClient:
    def __init__(
        self,
        base_url: str,
        demo_trading: bool,
        market_api: Any | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.demo_trading = demo_trading
        self.market_api = market_api or MarketData.MarketAPI(
            flag="1" if demo_trading else "0",
            domain=self.base_url,
        )

    def fetch_candles(
        self,
        symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
        limit: int,
    ) -> list[dict[str, Any]]:
        response = self.market_api.get_history_candlesticks(
            instId=symbol,
            before=str(int(end.astimezone(timezone.utc).timestamp() * 1000)),
            bar=interval,
            limit=str(limit),
        )
        if response.get("code") != "0":
            raise RuntimeError(f"OKX SDK error: {response}")
        rows = response.get("data", [])
        normalized: list[dict[str, Any]] = []
        for row in rows:
            timestamp_ms, open_price, high_price, low_price, close_price, volume, *_ = row
            candle_time = datetime.fromtimestamp(int(timestamp_ms) / 1000, tz=timezone.utc)
            if candle_time < start or candle_time > end:
                continue
            normalized.append(
                {
                    "symbol": symbol,
                    "exchange": "OKX",
                    "interval": interval,
                    "datetime": candle_time.isoformat(),
                    "open": float(open_price),
                    "high": float(high_price),
                    "low": float(low_price),
                    "close": float(close_price),
                    "volume": float(volume),
                }
            )
        return normalized


class HistorySyncService:
    def __init__(
        self,
        *,
        settings: Settings,
        history_client: HistoryClient,
        market_data_repository: MarketDataRepository,
        research_run_repository: ResearchRunRepository,
    ) -> None:
        self.settings = settings
        self.history_client = history_client
        self.market_data_repository = market_data_repository
        self.research_run_repository = research_run_repository

    def sync(
        self,
        *,
        symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
        fill_missing: bool,
    ) -> SyncResult:
        job_id = self.research_run_repository.create_sync_job(symbol, interval, start, end)
        try:
            retryer = Retrying(
                stop=stop_after_attempt(self.settings.research.sync_retry_count),
                wait=wait_fixed(0),
                reraise=True,
            )
            payloads = retryer(
                self.history_client.fetch_candles,
                symbol,
                interval,
                start,
                end,
                self.settings.research.sync_batch_limit,
            )
            cleaned = clean_bars(payloads, interval=interval, fill_missing=fill_missing)
            inserted = self.market_data_repository.upsert_bars(cleaned.bars)
            self.research_run_repository.complete_sync_job(
                job_id,
                status="completed",
                inserted_count=inserted,
                cleaned_count=len(cleaned.bars),
                duplicates_removed=cleaned.duplicates_removed,
            )
        except Exception as exc:
            self.research_run_repository.complete_sync_job(
                job_id,
                status="failed",
                error=str(exc),
            )
            raise
        return SyncResult(
            job_id=job_id,
            inserted_count=inserted,
            cleaned_count=len(cleaned.bars),
            duplicates_removed=cleaned.duplicates_removed,
            gaps_detected=cleaned.gaps_detected,
            gaps_filled=cleaned.gaps_filled,
        )
