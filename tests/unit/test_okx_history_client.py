from __future__ import annotations

from datetime import datetime, timezone

import pytest

from vntdr.services.history import OkxHistoryClient


class FakeMarketApi:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    def get_history_candlesticks(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "code": "0",
            "data": [
                ["1735696800000", "103", "105", "102", "104", "14", "0", "0", "1"],
                ["1735693200000", "102", "104", "101", "103", "13", "0", "0", "1"],
                ["1735691400000", "101", "103", "100", "102", "12", "0", "0", "1"],
                ["1735687800000", "100", "102", "99", "101", "11", "0", "0", "1"],
            ],
        }


def test_okx_history_client_uses_sdk_and_normalizes_30m_rows() -> None:
    market_api = FakeMarketApi()
    client = OkxHistoryClient(base_url="https://www.okx.com", demo_trading=False, market_api=market_api)

    rows = client.fetch_candles(
        symbol="XAUUSDT",
        interval="30m",
        start=datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
        end=datetime(2025, 1, 1, 1, 0, tzinfo=timezone.utc),
        limit=100,
    )

    assert len(market_api.calls) == 1
    assert market_api.calls[0]["instId"] == "XAUUSDT"
    assert market_api.calls[0]["bar"] == "30m"
    assert market_api.calls[0]["limit"] == "100"
    # Ensure after parameter is set to end timestamp (1735693200000) + 1000ms = 1735693201000
    assert market_api.calls[0]["after"] == "1735693201000"

    assert len(rows) == 2
    assert rows[0]["interval"] == "30m"
    assert rows[0]["close"] == 102.0


def test_okx_history_client_uses_public_market_flag_even_in_demo_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    created: dict[str, str] = {}

    class CapturingMarketApi(FakeMarketApi):
        def __init__(self, **kwargs) -> None:
            super().__init__()
            created.update(kwargs)

    monkeypatch.setattr("vntdr.services.history.MarketData.MarketAPI", CapturingMarketApi)

    OkxHistoryClient(base_url="https://www.okx.com", demo_trading=True)

    assert created["flag"] == "0"
    assert created["domain"] == "https://www.okx.com"
