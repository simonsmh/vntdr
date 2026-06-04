"""Telegram Bot 真实集成测试

不使用 Mock，直接测试真实的 API 调用和业务逻辑。
使用 pytest 标记 @pytest.mark.integration 来区分。
"""
from __future__ import annotations

import pytest

from vntdr.config import Settings
from vntdr.services.history import HistorySyncService, OkxHistoryClient
from vntdr.services.research import ResearchService
from vntdr.services.telegram_research import TelegramResearchService
from vntdr.storage.database import Database
from vntdr.storage.repositories import MarketDataRepository, ResearchRunRepository


@pytest.fixture
def settings(tmp_path):
    return Settings.from_mapping({
        "PG_HOST": "localhost",
        "PG_PORT": "5432",
        "PG_USER": "tester",
        "PG_PASSWORD": "secret",
        "PG_DB_NAME": "vntdr",
        "REDIS_HOST": "localhost",
        "REDIS_PORT": "6379",
        "REDIS_DB": "0",
        "VNTDR_REPORT_DIR": str(tmp_path / "reports"),
        "VNTDR_DEFAULT_WARMUP_DAYS": "10",
    })


@pytest.fixture
def sqlite_db(tmp_path):
    db_path = tmp_path / "test_db.sqlite3"
    database = Database(f"sqlite+pysqlite:///{db_path}")
    database.create_schema()
    return database


@pytest.fixture
def services(settings, sqlite_db):
    """创建真实的服务实例（不使用 Mock）"""
    market_repo = MarketDataRepository(sqlite_db)
    research_run_repo = ResearchRunRepository(sqlite_db)
    research_service = ResearchService(
        settings=settings,
        market_data_repository=market_repo,
        research_run_repository=research_run_repo,
    )
    history_service = HistorySyncService(
        settings=settings,
        history_client=OkxHistoryClient(
            base_url=settings.okx.rest_base_url,
            demo_trading=settings.okx.demo_trading,
        ),
        market_data_repository=market_repo,
        research_run_repository=research_run_repo,
    )
    telegram_research = TelegramResearchService(
        settings=settings,
        history_service=history_service,
        research_service=research_service,
    )
    return {
        "research": research_service,
        "history": history_service,
        "telegram_research": telegram_research,
    }


class TestOKXApiIntegration:
    """测试 OKX API 真实调用"""

    @pytest.mark.integration
    def test_okx_bar_parameter_case_sensitivity(self, services):
        """测试 OKX API bar 参数大小写问题 - 真实调用验证"""
        history_service = services["history"]

        # 测试小写的 interval 是否被正确转换
        # 这是之前导致 "Parameter bar error" 的原因
        symbol = "XAU-USDT-SWAP"

        # 测试各种大小写组合
        test_intervals = ["1h", "1H", "4h", "4H", "15m", "30m"]

        for interval in test_intervals:
            try:
                from datetime import datetime, timedelta, timezone
                end = datetime.now(timezone.utc)
                start = end - timedelta(hours=1)

                result = history_service.history_client.fetch_candles(
                    symbol=symbol,
                    interval=interval,
                    start=start,
                    end=end,
                    limit=10,
                )
                print(f"✓ interval='{interval}' 成功，获取了 {len(result)} 根 K 线")
                # 只要不抛异常就是成功
            except RuntimeError as e:
                if "Parameter bar error" in str(e):
                    pytest.fail(f"❌ OKX API 仍然报错！interval='{interval}': {e}")
                else:
                    # 其他错误可能是网络问题，跳过
                    pytest.skip(f"其他错误（可能是网络问题）: {e}")

    @pytest.mark.integration
    def test_telegram_research_rank_intervals_with_defaults(self, services):
        """测试 TelegramResearchService.rank_intervals - 完整流程"""
        telegram_research = services["telegram_research"]

        # 测试真实的排名流程
        symbol = "XAU-USDT-SWAP"
        strategy_name = "cm_macd_ult_mtf"
        method = "ga"

        # 使用默认的间隔和回看时间
        intervals = telegram_research.available_intervals()
        lookback_hours = 24

        print(f"\n测试排名计算: {symbol} {strategy_name}")
        print(f"间隔: {intervals}, 回看: {lookback_hours}小时")

        try:
            results = telegram_research.rank_intervals(
                symbol=symbol,
                strategy_name=strategy_name,
                method=method,
                intervals=intervals,
                lookback_hours=lookback_hours,
            )

            print(f"✓ rank_intervals 成功，返回 {len(results)} 个结果")
            for r in results:
                print(f"  - {r.interval}: return={r.total_return:.4f}, sharpe={r.sharpe_ratio:.4f}")

            assert len(results) > 0, "应该至少返回一个结果"

        except RuntimeError as e:
            if "Parameter bar error" in str(e):
                pytest.fail(f"❌ OKX API bar 参数问题未修复: {e}")
            raise

    @pytest.mark.integration
    def test_available_intervals_format(self, services):
        """验证 available_intervals 返回的格式是否正确"""
        telegram_research = services["telegram_research"]

        intervals = telegram_research.available_intervals()

        print(f"\navailable_intervals 返回: {intervals}")

        # 验证所有间隔都在 INTERVAL_TO_DELTA 中
        from vntdr.cleaning import INTERVAL_TO_DELTA

        for interval in intervals:
            assert interval in INTERVAL_TO_DELTA, f"interval '{interval}' 不在 INTERVAL_TO_DELTA 中"

        # 验证至少包含常用周期
        assert "15m" in intervals
        assert "30m" in intervals
        assert "1h" in intervals
        assert "4h" in intervals


def run_real_integration_tests():
    """手动运行真实集成测试"""
    import sys

    result = pytest.main([
        __file__,
        "-v",
        "-s",  # 显示 print 输出
        "--tb=short",
        "-m", "integration",
    ])
    sys.exit(result)


if __name__ == "__main__":
    run_real_integration_tests()
