from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from vntdr.services.tradingview_history import (
    TradingViewHistoryClient,
    decode_frames,
    frame_message,
    tradingview_resolution,
)


class FakeConnection:
    def __init__(self, responses: list[str]) -> None:
        self.responses = iter(responses)
        self.sent: list[str] = []
        self.closed = False

    def send(self, message: str) -> None:
        self.sent.append(message)

    def recv(self) -> str:
        return next(self.responses)

    def close(self) -> None:
        self.closed = True


def test_protocol_frames_round_trip() -> None:
    first = frame_message("set_auth_token", ["token"])
    second = frame_message("chart_create_session", ["cs_test", ""])

    assert decode_frames(first + second) == [
        {"m": "set_auth_token", "p": ["token"]},
        {"m": "chart_create_session", "p": ["cs_test", ""]},
    ]


@pytest.mark.parametrize(
    ("interval", "resolution"),
    [("15m", "15"), ("1h", "60"), ("4H", "240"), ("1d", "1D"), ("1w", "1W")],
)
def test_tradingview_resolution(interval: str, resolution: str) -> None:
    assert tradingview_resolution(interval) == resolution


def test_history_client_normalizes_and_isolates_proxy_bars() -> None:
    update = frame_message(
        "timescale_update",
        [
            "cs_test",
            {
                "s1": {
                    "s": [
                        {"i": 0, "v": [1767225600, 100, 105, 99, 103, 42]},
                        {"i": 1, "v": [1767240000, 103, 107, 102, 106, None]},
                    ]
                }
            },
        ],
    )
    completed = frame_message("series_completed", ["cs_test", "s1"])
    connection = FakeConnection([update, completed])
    factory_args: dict[str, object] = {}

    def factory(url: str, **kwargs: object) -> FakeConnection:
        factory_args.update(url=url, **kwargs)
        return connection

    client = TradingViewHistoryClient(
        tradingview_symbol="OANDA:XAUUSD",
        output_symbol="TV:XAUUSD",
        connection_factory=factory,
    )
    rows = client.fetch_candles(
        "TV:XAUUSD",
        "4h",
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        datetime(2026, 1, 2, tzinfo=timezone.utc),
        100,
    )

    assert factory_args["origin"] == "https://www.tradingview.com"
    assert connection.closed
    assert [row["exchange"] for row in rows] == ["TRADINGVIEW", "TRADINGVIEW"]
    assert [row["symbol"] for row in rows] == ["TV:XAUUSD", "TV:XAUUSD"]
    assert rows[1]["volume"] == 0.0
    decoded_sent = [decode_frames(message)[0] for message in connection.sent]
    methods = [message["m"] for message in decoded_sent]
    assert methods == [
        "set_auth_token",
        "chart_create_session",
        "quote_create_session",
        "quote_add_symbols",
        "resolve_symbol",
        "create_series",
    ]
    create_series = decoded_sent[-1]
    assert create_series["p"][4] == "240"


def test_history_client_rejects_non_isolated_output_symbol() -> None:
    with pytest.raises(ValueError, match="TV:"):
        TradingViewHistoryClient(
            tradingview_symbol="OANDA:XAUUSD",
            output_symbol="XAU-USDT-SWAP",
        )


def test_history_client_requests_older_pages_until_start_is_covered() -> None:
    newest = frame_message(
        "timescale_update",
        ["cs", {"s1": {"s": [{"i": 1, "v": [1767240000, 2, 2, 2, 2, 2]}]}}],
    )
    older = frame_message(
        "timescale_update",
        ["cs", {"s1": {"s": [{"i": 0, "v": [1767225600, 1, 1, 1, 1, 1]}]}}],
    )
    completed = frame_message("series_completed", ["cs", "s1"])
    connection = FakeConnection([newest, completed, older, completed])
    client = TradingViewHistoryClient(
        tradingview_symbol="OANDA:XAUUSD",
        output_symbol="TV:XAUUSD",
        connection_factory=lambda *args, **kwargs: connection,
    )

    rows = client.fetch_candles(
        "TV:XAUUSD",
        "4h",
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        datetime(2026, 1, 2, tzinfo=timezone.utc),
        2,
    )

    methods = [
        decode_frames(message)[0]["m"]
        for message in connection.sent
        if decode_frames(message)
    ]
    assert "request_more_data" in methods
    assert len(rows) == 2


def test_frames_are_compact_json() -> None:
    framed = frame_message("method", [{"a": 1}])
    payload = decode_frames(framed)[0]
    assert json.dumps(payload, separators=(",", ":")) in framed
