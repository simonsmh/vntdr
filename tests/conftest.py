from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest


@pytest.fixture
def env_map(tmp_path: Path) -> dict[str, str]:
    return {
        "PG_HOST": "localhost",
        "PG_PORT": "5432",
        "PG_USER": "tester",
        "PG_PASSWORD": "secret",
        "PG_DB_NAME": "vntdr",
        "REDIS_HOST": "localhost",
        "REDIS_PORT": "6379",
        "REDIS_DB": "0",
        "VNTDR_REPORT_DIR": str(tmp_path / "reports"),
        "VNTDR_SYNC_RETRY_COUNT": "3",
        "VNTDR_SYNC_BATCH_LIMIT": "100",
        "VNTDR_DEFAULT_WARMUP_DAYS": "10",
    }


@pytest.fixture
def sample_bar_payloads() -> list[dict[str, Any]]:
    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    prices = [100, 101, 102, 101, 103, 104, 103, 105, 106, 108]
    payloads: list[dict[str, Any]] = []
    for index, close in enumerate(prices):
        timestamp = start + timedelta(minutes=index)
        payloads.append(
            {
                "symbol": "BTC-USDT-SWAP",
                "exchange": "OKX",
                "interval": "1m",
                "datetime": timestamp.isoformat(),
                "open": close - 1,
                "high": close + 1,
                "low": close - 2,
                "close": close,
                "volume": 10 + index,
            }
        )
    return payloads


@pytest.fixture
def sample_xau_bar_payloads() -> list[dict[str, Any]]:
    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    closes = [
        100.0,
        101.0,
        102.0,
        104.0,
        107.0,
        110.0,
        112.0,
        111.0,
        108.0,
        104.0,
        100.0,
        96.0,
        94.0,
        95.0,
        98.0,
        102.0,
        106.0,
        111.0,
        115.0,
        118.0,
        116.0,
        111.0,
        106.0,
        101.0,
    ]
    payloads: list[dict[str, Any]] = []
    for index, close in enumerate(closes):
        timestamp = start + timedelta(hours=index * 4)
        payloads.append(
            {
                "symbol": "XAUUSDT",
                "exchange": "OKX",
                "interval": "4h",
                "datetime": timestamp.isoformat(),
                "open": close - 0.8,
                "high": close + 1.2,
                "low": close - 1.5,
                "close": close,
                "volume": 20 + index,
            }
        )
    return payloads
