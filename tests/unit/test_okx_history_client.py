from __future__ import annotations

from datetime import datetime, timezone

from vntdr.services.history import OkxHistoryClient


class FakeMarketApi:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    def get_history_candlesticks(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "code": "0",
            "data": [
                ["1735691400000", "101", "103", "100", "102", "12", "0", "0", "1"],
                ["1735693200000", "102", "104", "101", "103", "13", "0", "0", "1"],
                ["1735696800000", "103", "105", "102", "104", "14", "0", "0", "1"],
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

    assert market_api.calls == [
        {
            "instId": "XAUUSDT",
            "before": "1735693200000",
            "bar": "30m",
            "limit": "100",
        }
    ]
    assert len(rows) == 2
    assert rows[0]["interval"] == "30m"
    assert rows[0]["close"] == 102.0
