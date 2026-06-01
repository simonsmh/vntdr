from datetime import datetime, timezone
import pytest
from vntdr.models import BarRecord
from vntdr.storage.repositories import MarketDataRepository
from vntdr.storage.database import Database

@pytest.fixture
def database(tmp_path):
    db_path = tmp_path / "test_repo.sqlite3"
    db = Database(f"sqlite+pysqlite:///{db_path}")
    db.create_schema()
    return db

def test_fetch_latest_bars_interval_case_insensitivity(database: Database):
    repo = MarketDataRepository(database)
    symbol = "XAU-USDT-SWAP"
    
    # 1. 存入小写 interval 的数据
    bars_lower = [
        BarRecord(
            symbol=symbol,
            exchange="okx",
            interval="4h",
            datetime=datetime(2026, 4, 23, 10, 0, tzinfo=timezone.utc),
            open=2000.0, high=2010.0, low=1990.0, close=2005.0, volume=100.0
        )
    ]
    repo.upsert_bars(bars_lower)
    
    # 2. 存入大写 interval 的数据
    bars_upper = [
        BarRecord(
            symbol=symbol,
            exchange="okx",
            interval="4H",
            datetime=datetime(2026, 4, 23, 14, 0, tzinfo=timezone.utc),
            open=2005.0, high=2015.0, low=2000.0, close=2012.0, volume=150.0
        )
    ]
    repo.upsert_bars(bars_upper)
    
    # 3. 验证使用大写 "4H" 查询时能查到所有数据（之前只会查到 4H 那一条）
    results = repo.fetch_latest_bars(symbol, "4H", limit=10)
    assert len(results) >= 2
    intervals = {b.interval for b in results}
    assert "4h" in intervals
    assert "4H" in intervals
    
    # 4. 验证排序是否正确（最新的在最后，因为 fetch_latest_bars 内部对数据库返回的 desc 做了 reversed）
    assert results[-1].datetime == datetime(2026, 4, 23, 14, 0, tzinfo=timezone.utc)
    assert results[-2].datetime == datetime(2026, 4, 23, 10, 0, tzinfo=timezone.utc)

def test_fetch_bars_range_interval_case_insensitivity(database: Database):
    repo = MarketDataRepository(database)
    symbol = "BTC-USDT-SWAP"
    
    repo.upsert_bars([
        BarRecord(
            symbol=symbol, exchange="okx", interval="1h",
            datetime=datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc),
            open=100.0, high=110.0, low=90.0, close=105.0, volume=1.0
        ),
        BarRecord(
            symbol=symbol, exchange="okx", interval="1H",
            datetime=datetime(2026, 1, 1, 11, 0, tzinfo=timezone.utc),
            open=105.0, high=115.0, low=100.0, close=110.0, volume=1.0
        )
    ])
    
    # 使用小写查询范围
    results = repo.fetch_bars(
        symbol, "1h", 
        start=datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
        end=datetime(2026, 1, 1, 23, 0, tzinfo=timezone.utc)
    )
    assert len(results) == 2
    
    # 使用大写查询范围
    results_upper = repo.fetch_bars(
        symbol, "1H", 
        start=datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
        end=datetime(2026, 1, 1, 23, 0, tzinfo=timezone.utc)
    )
    assert len(results_upper) == 2
