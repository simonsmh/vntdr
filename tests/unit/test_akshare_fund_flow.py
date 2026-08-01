from __future__ import annotations

from datetime import date

import pandas as pd

import vntdr.services.akshare_fund_flow as akshare_fund_flow
from vntdr.services.akshare_fund_flow import (
    AkShareFlowConfig,
    AkShareFundFlowProvider,
    normalize_flow_frame,
    summarize_flow_trend,
)


def _row(day: str, main: float, extra_large: float, large: float, symbol: str = "000001") -> dict:
    return {
        "日期": day,
        "主力净流入-净额": main,
        "主力净流入-净占比": main / 1000,
        "超大单净流入-净额": extra_large,
        "大单净流入-净额": large,
        "大单净流入-净占比": large / 1000,
    }


def test_normalize_flow_keeps_components_and_flags_gap() -> None:
    frame = normalize_flow_frame(
        pd.DataFrame(
            [
                _row("2026-07-01", 150.0, 100.0, 50.0),
                _row("2026-07-02", -20.0, -5.0, -10.0),
            ]
        ),
        symbol="1",
    )
    assert list(frame["trade_date"]) == [date(2026, 7, 1), date(2026, 7, 2)]
    assert list(frame["symbol"]) == ["000001", "000001"]
    assert frame.loc[0, "calculated_main_net_inflow"] == 150.0
    assert frame.loc[1, "main_component_gap"] == -5.0


def test_summarize_flow_reports_daily_breadth_and_direction() -> None:
    frame = pd.concat(
        [
            normalize_flow_frame(
                pd.DataFrame([_row("2026-07-01", 100, 60, 40)]), symbol="000001"
            ),
            normalize_flow_frame(
                pd.DataFrame([_row("2026-07-01", -20, -10, -10)]), symbol="000002"
            ),
            normalize_flow_frame(
                pd.DataFrame([_row("2026-07-02", 200, 100, 100)]), symbol="000001"
            ),
            normalize_flow_frame(
                pd.DataFrame([_row("2026-07-02", 50, 30, 20)]), symbol="000002"
            ),
            normalize_flow_frame(
                pd.DataFrame([_row("2026-07-03", 300, 150, 150)]), symbol="000001"
            ),
            normalize_flow_frame(
                pd.DataFrame([_row("2026-07-03", 80, 40, 40)]), symbol="000002"
            ),
            normalize_flow_frame(
                pd.DataFrame([_row("2026-07-04", 400, 200, 200)]), symbol="000001"
            ),
            normalize_flow_frame(
                pd.DataFrame([_row("2026-07-04", 100, 50, 50)]), symbol="000002"
            ),
            normalize_flow_frame(
                pd.DataFrame([_row("2026-07-05", 500, 250, 250)]), symbol="000001"
            ),
            normalize_flow_frame(
                pd.DataFrame([_row("2026-07-05", 120, 60, 60)]), symbol="000002"
            ),
        ],
        ignore_index=True,
    )
    daily, stocks, summary = summarize_flow_trend(frame)
    assert len(daily) == 5
    assert daily.loc[0, "main_positive_ratio"] == 0.5
    assert summary["stock_count"] == 2
    assert summary["trend"] == "转强"
    assert stocks.iloc[0]["symbol"] == "000001"


def test_fetch_one_retries_wrapper_and_fallback_then_succeeds(monkeypatch) -> None:
    class FlakyAkShare:
        calls = 0

        def stock_individual_fund_flow(self, *, stock: str, market: str) -> pd.DataFrame:
            del stock, market
            self.calls += 1
            if self.calls < 3:
                raise ConnectionError("remote end closed connection")
            return pd.DataFrame([_row("2026-07-01", 100.0, 60.0, 40.0)])

    def fail_fallback(symbol: str, market: str) -> pd.DataFrame:
        del symbol, market
        raise ConnectionError("fallback unavailable")

    monkeypatch.setattr(akshare_fund_flow, "_fetch_public_flow_frame", fail_fallback)
    sleep_calls: list[float] = []
    provider = AkShareFundFlowProvider(
        ak_module=FlakyAkShare(),
        config=AkShareFlowConfig(
            max_retries=2,
            retry_backoff_seconds=0.5,
            retry_jitter_seconds=0.0,
        ),
        sleep=sleep_calls.append,
    )

    result = provider._fetch_one("000001", "sz")

    assert len(result) == 1
    assert sleep_calls == [0.5, 1.0]
    assert provider._retry_count == 2


def test_fetch_etf_universe_filters_total_market_cap() -> None:
    class FakeAkShare:
        def fund_etf_spot_em(self) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    {"代码": "510300", "名称": "沪深300ETF", "总市值": 12_000_000_000},
                    {"代码": "159845", "名称": "中证1000ETF", "总市值": 9_900_000_000},
                    {"代码": "588200", "名称": "科创芯片ETF", "总市值": 10_000_000_000},
                ]
            )

    provider = AkShareFundFlowProvider(
        ak_module=FakeAkShare(),
        config=AkShareFlowConfig(max_retries=0),
    )
    result = provider.fetch_etf_universe(min_market_cap=10_000_000_000)

    assert list(result["symbol"]) == ["510300", "588200"]
    assert list(result["market"]) == ["sh", "sh"]
