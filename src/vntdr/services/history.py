from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Protocol
import asyncio
from concurrent.futures import ThreadPoolExecutor

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
        # Public market data should always use OKX's normal public market
        # endpoint. Some instruments, such as QQQ-USDT-SWAP, are unavailable
        # when the simulated-trading header is attached even though account
        # and order APIs should still use demo mode.
        self.market_api = market_api or MarketData.MarketAPI(
            flag="0",
            domain=self.base_url,
        )
        self._executor = ThreadPoolExecutor(max_workers=4)

    def fetch_candles(
        self,
        symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
        limit: int,
    ) -> list[dict[str, Any]]:
        import time
        all_rows: list[dict[str, Any]] = []
        
        # OKX requires bar in uppercase for hours: 1h -> 1H, 4h -> 4H
        okx_bar = interval.upper() if interval.endswith(('h', 'H')) else interval
        
        # Convert start and end to timezone-aware UTC
        start_utc = start.astimezone(timezone.utc)
        end_utc = end.astimezone(timezone.utc)
        
        # We will page backward using the after parameter
        # OKX candles are returned sorted from newest to oldest
        current_after = int(end_utc.timestamp() * 1000) + 1000 # Add 1s buffer
        
        while True:
            response = self.market_api.get_history_candlesticks(
                instId=symbol,
                bar=okx_bar,
                after=str(current_after),
                limit=str(100),
            )
            if response.get("code") != "0":
                raise RuntimeError(f"OKX SDK error: {response}")
                
            rows = response.get("data", [])
            if not rows:
                break
                
            oldest_ts = None
            has_reached_start = False
            
            for row in rows:
                timestamp_ms, open_price, high_price, low_price, close_price, volume, *_ = row
                ts_val = int(timestamp_ms)
                candle_time = datetime.fromtimestamp(ts_val / 1000, tz=timezone.utc)
                
                if oldest_ts is None or ts_val < oldest_ts:
                    oldest_ts = ts_val
                    
                if candle_time < start_utc:
                    has_reached_start = True
                    continue
                    
                if candle_time > end_utc:
                    continue
                    
                all_rows.append(
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
                
            if has_reached_start or oldest_ts is None or oldest_ts >= current_after:
                break
                
            current_after = oldest_ts
            time.sleep(0.1) # Avoid hitting OKX API rate limits (20 req / 2s)
            
        all_rows.sort(key=lambda x: x["datetime"])
        return all_rows

    async def fetch_candles_async(
        self,
        symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
        limit: int,
    ) -> list[dict[str, Any]]:
        return await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self.fetch_candles,
            symbol,
            interval,
            start,
            end,
            limit
        )


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
        self._executor = ThreadPoolExecutor(max_workers=4)

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

    async def sync_async(
        self,
        *,
        symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
        fill_missing: bool,
    ) -> SyncResult:
        return await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self.sync,
            symbol,
            interval,
            start,
            end,
            fill_missing
        )
