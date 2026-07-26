from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from vntdr.config import Settings
from vntdr.models import BarRecord
from vntdr.services.research import ResearchService
from vntdr.storage.database import Database
from vntdr.storage.repositories import MarketDataRepository, ResearchRunRepository


class AlwaysLong:
    @classmethod
    def signal_for_index(cls, bars, index, parameters):
        return 1


class EnterThenFlat:
    @classmethod
    def signal_for_index(cls, bars, index, parameters):
        return 1 if index == 0 else 0


class NeedsWarmup:
    @classmethod
    def signal_for_index(cls, bars, index, parameters):
        return 1 if index >= 3 else 0


class Alternating:
    @classmethod
    def signal_for_index(cls, bars, index, parameters):
        return 1 if index % 2 == 0 else -1


def test_backtest_fills_a_close_signal_at_next_open_and_records_one_complete_trade(tmp_path, env_map, monkeypatch) -> None:
    db = Database(f"sqlite+pysqlite:///{tmp_path / 'db.sqlite'}")
    db.create_schema()
    settings = Settings.from_mapping({**env_map, "VNTDR_DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'db.sqlite'}"})
    service = ResearchService(settings=settings, market_data_repository=MarketDataRepository(db), research_run_repository=ResearchRunRepository(db))
    monkeypatch.setattr(service, "_load_strategy", lambda _: AlwaysLong)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    bars = [
        BarRecord(symbol="BTC", interval="1h", datetime=start + timedelta(hours=i), open=open_, high=open_ + 1, low=open_ - 1, close=close_)
        for i, (open_, close_) in enumerate([(100, 100), (110, 111), (120, 121)])
    ]
    outcome = service._execute_backtest(bars, "always_long", {})
    assert outcome.trades is not None and len(outcome.trades) == 1
    assert outcome.trades[0].entry_price == 110
    assert outcome.trades[0].exit_price == 121
    assert outcome.metrics["trade_count"] == 1.0


def test_each_bar_transition_has_one_cost_inclusive_return_observation(
    tmp_path, env_map, monkeypatch
) -> None:
    db = Database(f"sqlite+pysqlite:///{tmp_path / 'periods.sqlite'}")
    db.create_schema()
    settings = Settings.from_mapping(
        {
            **env_map,
            "VNTDR_DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'periods.sqlite'}",
            "VNTDR_TAKER_FEE_RATE": "0.01",
        }
    )
    service = ResearchService(
        settings=settings,
        market_data_repository=MarketDataRepository(db),
        research_run_repository=ResearchRunRepository(db),
    )
    monkeypatch.setattr(service, "_load_strategy", lambda _: EnterThenFlat)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    bars = [
        BarRecord(
            symbol="BTC", interval="1h", datetime=start + timedelta(hours=i),
            open=100, high=100, low=100, close=100,
        )
        for i in range(5)
    ]

    outcome = service._execute_backtest(bars, "enter_then_flat", {})

    assert len(outcome.equity_curve) == len(bars)
    # Two fees occur during the second transition and must not create extra
    # pseudo-periods. Remaining flat transitions are retained as zero returns.
    period_returns = [
        outcome.equity_curve[i] / outcome.equity_curve[i - 1] - 1
        for i in range(1, len(outcome.equity_curve))
    ]
    assert len(period_returns) == len(bars) - 1
    assert period_returns[0] == pytest.approx(-0.01)
    assert period_returns[1] == pytest.approx(-0.01)
    assert period_returns[2:] == pytest.approx([0.0, 0.0])


def test_out_of_sample_window_keeps_training_bars_as_signal_warmup(
    tmp_path, env_map, monkeypatch
) -> None:
    db = Database(f"sqlite+pysqlite:///{tmp_path / 'warmup.sqlite'}")
    db.create_schema()
    settings = Settings.from_mapping(
        {
            **env_map,
            "VNTDR_DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'warmup.sqlite'}",
            "VNTDR_TAKER_FEE_RATE": "0",
        }
    )
    service = ResearchService(
        settings=settings,
        market_data_repository=MarketDataRepository(db),
        research_run_repository=ResearchRunRepository(db),
    )
    monkeypatch.setattr(service, "_load_strategy", lambda _: NeedsWarmup)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    bars = [
        BarRecord(
            symbol="BTC", interval="1h", datetime=start + timedelta(hours=i),
            open=100 + i, high=101 + i, low=99 + i, close=100 + i,
        )
        for i in range(7)
    ]

    outcome = service._execute_backtest(
        bars,
        "needs_warmup",
        {},
        decision_start_index=4,
    )

    assert outcome.signals == [1, 1]
    assert len(outcome.equity_curve) == 3
    assert outcome.trades and outcome.trades[0].entry_time == bars[5].datetime


def test_minimum_holding_and_cooldown_prevent_immediate_reversals(tmp_path, env_map, monkeypatch) -> None:
    db = Database(f"sqlite+pysqlite:///{tmp_path / 'governance.sqlite'}")
    db.create_schema()
    settings = Settings.from_mapping({**env_map, "VNTDR_DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'governance.sqlite'}", "VNTDR_TAKER_FEE_RATE": "0"})
    service = ResearchService(settings=settings, market_data_repository=MarketDataRepository(db), research_run_repository=ResearchRunRepository(db))
    monkeypatch.setattr(service, "_load_strategy", lambda _: Alternating)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    bars = [BarRecord(symbol="BTC", interval="1h", datetime=start + timedelta(hours=i), open=100, high=100, low=100, close=100) for i in range(10)]

    outcome = service._execute_backtest(
        bars, "alternating", {"min_holding_bars": 3, "cooldown_bars": 2}
    )

    # Without governance every bar reverses; holding/cooldown permits only a
    # small number of complete trades over the same sequence.
    assert outcome.trades is not None and len(outcome.trades) == 2
    assert all(trade.bars_held >= 3 for trade in outcome.trades)


def test_atr_risk_sizing_scales_returns_costs_and_trade_ledger(tmp_path, env_map, monkeypatch) -> None:
    db = Database(f"sqlite+pysqlite:///{tmp_path / 'sizing.sqlite'}")
    db.create_schema()
    settings = Settings.from_mapping({**env_map, "VNTDR_DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'sizing.sqlite'}", "VNTDR_TAKER_FEE_RATE": "0"})
    service = ResearchService(settings=settings, market_data_repository=MarketDataRepository(db), research_run_repository=ResearchRunRepository(db))
    monkeypatch.setattr(service, "_load_strategy", lambda _: AlwaysLong)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    bars = [
        BarRecord(symbol="BTC", interval="1h", datetime=start + timedelta(hours=index), open=price, high=price + 1, low=price - 1, close=price)
        for index, price in enumerate([100, 100, 105, 110])
    ]

    outcome = service._execute_backtest(
        bars, "always_long", {"enable_atr_sizing": True, "risk_fraction": 0.01, "stop_atr_multiple": 2.0, "max_notional_fraction": 0.30},
    )

    assert outcome.trades is not None
    # ATR is 2; 1% risk over a 2 ATR stop gives 25% notional exposure.
    assert outcome.trades[0].gross_return == pytest.approx(0.025)
    assert 0.02 < outcome.metrics["total_return"] < 0.025
