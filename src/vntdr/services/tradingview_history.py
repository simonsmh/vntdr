"""Unofficial TradingView WebSocket history adapter.

Protocol behavior is based on the MIT-licensed ``rongardF/tvdatafeed`` project:
https://github.com/rongardF/tvdatafeed

TradingView does not publish this browser protocol as a supported market-data
API. Keep records under a distinct ``TV:`` local symbol and the
``TRADINGVIEW`` exchange so research proxy data cannot be mistaken for bars
from an executable venue.
"""

from __future__ import annotations

import json
import math
import secrets
from datetime import datetime, timezone
from typing import Any, Callable

import websocket

from vntdr.models import Interval

TRADINGVIEW_WEBSOCKET_URL = "wss://data.tradingview.com/socket.io/websocket"
UNAUTHORIZED_USER_TOKEN = "unauthorized_user_token"


def frame_message(method: str, params: list[Any]) -> str:
    payload = json.dumps({"m": method, "p": params}, separators=(",", ":"))
    return f"~m~{len(payload)}~m~{payload}"


def decode_frames(raw: str) -> list[dict[str, Any]]:
    """Decode one or more TradingView ``~m~length~m~`` protocol frames."""
    messages: list[dict[str, Any]] = []
    cursor = 0
    while True:
        marker = raw.find("~m~", cursor)
        if marker < 0:
            break
        length_end = raw.find("~m~", marker + 3)
        if length_end < 0:
            break
        try:
            payload_length = int(raw[marker + 3 : length_end])
        except ValueError:
            cursor = length_end + 3
            continue
        payload_start = length_end + 3
        payload = raw[payload_start : payload_start + payload_length]
        if len(payload) != payload_length:
            break
        cursor = payload_start + payload_length
        try:
            decoded = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if isinstance(decoded, dict):
            messages.append(decoded)
    return messages


def tradingview_resolution(interval: str) -> str:
    normalized = Interval(value=interval).value
    amount, unit = int(normalized[:-1]), normalized[-1]
    if unit == "m":
        return str(amount)
    if unit == "h":
        return str(amount * 60)
    if unit == "d":
        return f"{amount}D"
    if unit == "w":
        return f"{amount}W"
    raise ValueError(f"Unsupported TradingView interval: {interval}")


class TradingViewHistoryClient:
    """Fetch TradingView chart bars through its unofficial browser WebSocket."""

    def __init__(
        self,
        *,
        tradingview_symbol: str,
        output_symbol: str,
        auth_token: str | None = None,
        extended_session: bool = False,
        timeout_seconds: float = 20.0,
        max_bars: int = 20_000,
        websocket_url: str = TRADINGVIEW_WEBSOCKET_URL,
        connection_factory: Callable[..., Any] = websocket.create_connection,
    ) -> None:
        if ":" not in tradingview_symbol:
            raise ValueError("tradingview_symbol must use EXCHANGE:SYMBOL syntax")
        if not output_symbol.startswith("TV:"):
            raise ValueError("output_symbol must start with 'TV:' to isolate proxy data")
        self.tradingview_symbol = tradingview_symbol
        self.output_symbol = output_symbol
        self.auth_token = auth_token or UNAUTHORIZED_USER_TOKEN
        self.extended_session = extended_session
        self.timeout_seconds = timeout_seconds
        self.max_bars = max_bars
        self.websocket_url = websocket_url
        self.connection_factory = connection_factory

    @staticmethod
    def _session(prefix: str) -> str:
        return f"{prefix}_{secrets.token_hex(6)}"

    @staticmethod
    def _bar_count(start: datetime, end: datetime, interval: str, limit: int) -> int:
        if end <= start:
            raise ValueError("end must be after start")
        seconds = Interval(value=interval).seconds
        requested = math.ceil((end - start).total_seconds() / seconds) + 2
        return max(limit, requested)

    @staticmethod
    def _extract_bars(message: dict[str, Any]) -> list[list[Any]]:
        if message.get("m") != "timescale_update":
            return []
        params = message.get("p", [])
        if len(params) < 2 or not isinstance(params[1], dict):
            return []
        bars: list[list[Any]] = []
        for series in params[1].values():
            if not isinstance(series, dict):
                continue
            for item in series.get("s", []):
                values = item.get("v") if isinstance(item, dict) else None
                if isinstance(values, list) and len(values) >= 5:
                    bars.append(values)
        return bars

    def fetch_candles(
        self,
        symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
        limit: int,
    ) -> list[dict[str, Any]]:
        if symbol != self.output_symbol:
            raise ValueError(f"symbol must be the isolated local symbol {self.output_symbol!r}")
        start_utc = start.replace(tzinfo=timezone.utc) if start.tzinfo is None else start.astimezone(timezone.utc)
        end_utc = end.replace(tzinfo=timezone.utc) if end.tzinfo is None else end.astimezone(timezone.utc)
        count = min(self._bar_count(start_utc, end_utc, interval, limit), self.max_bars)
        chart_session = self._session("cs")
        quote_session = self._session("qs")
        connection = self.connection_factory(
            self.websocket_url,
            timeout=self.timeout_seconds,
            origin="https://www.tradingview.com",
        )

        def send(method: str, params: list[Any]) -> None:
            connection.send(frame_message(method, params))

        rows: dict[int, dict[str, Any]] = {}
        completed_row_count = 0
        try:
            send("set_auth_token", [self.auth_token])
            send("chart_create_session", [chart_session, ""])
            send("quote_create_session", [quote_session])
            send(
                "quote_add_symbols",
                [quote_session, self.tradingview_symbol, {"flags": ["force_permission"]}],
            )
            session = "extended" if self.extended_session else "regular"
            resolved = json.dumps(
                {
                    "symbol": self.tradingview_symbol,
                    "adjustment": "splits",
                    "session": session,
                },
                separators=(",", ":"),
            )
            send("resolve_symbol", [chart_session, "symbol_1", f"={resolved}"])
            send(
                "create_series",
                [
                    chart_session,
                    "s1",
                    "s1",
                    "symbol_1",
                    tradingview_resolution(interval),
                    count,
                ],
            )
            while True:
                raw = connection.recv()
                if not isinstance(raw, str):
                    continue
                if "~h~" in raw:
                    # Browser clients echo TradingView heartbeat frames.
                    connection.send(raw)
                    continue
                for message in decode_frames(raw):
                    for values in self._extract_bars(message):
                        try:
                            timestamp = int(float(values[0]))
                            candle_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                            if not start_utc <= candle_time <= end_utc:
                                continue
                            rows[timestamp] = {
                                "symbol": self.output_symbol,
                                "exchange": "TRADINGVIEW",
                                "interval": Interval(value=interval).value,
                                "datetime": candle_time.isoformat(),
                                "open": float(values[1]),
                                "high": float(values[2]),
                                "low": float(values[3]),
                                "close": float(values[4]),
                                "volume": float(values[5] or 0.0) if len(values) > 5 else 0.0,
                            }
                        except (TypeError, ValueError, OverflowError):
                            continue
                    if message.get("m") == "series_completed":
                        oldest_timestamp = min(rows) if rows else None
                        needs_older = (
                            oldest_timestamp is not None
                            and datetime.fromtimestamp(oldest_timestamp, tz=timezone.utc) > start_utc
                            and len(rows) < count
                        )
                        made_progress = len(rows) > completed_row_count
                        if needs_older and made_progress:
                            completed_row_count = len(rows)
                            send(
                                "request_more_data",
                                [chart_session, "s1", min(5_000, count - len(rows))],
                            )
                            continue
                        return [rows[key] for key in sorted(rows)]
        finally:
            connection.close()
