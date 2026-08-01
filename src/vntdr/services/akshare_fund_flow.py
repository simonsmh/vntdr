"""AkShare-backed A-share money-flow research for the MVP.

AkShare is intentionally kept behind a small provider boundary.  The current
implementation uses the public Eastmoney/CSI endpoints exposed by AkShare and
does not feed this research-only data into the OKX execution path.
"""
from __future__ import annotations

import importlib
import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from typing import Any

import httpx
import pandas as pd

LOGGER = logging.getLogger(__name__)


class AkShareUnavailableError(RuntimeError):
    """Raised when the optional AkShare dependency is not installed."""


class AkShareDataError(RuntimeError):
    """Raised when an AkShare response cannot be normalized safely."""


@dataclass(frozen=True)
class AkShareFlowConfig:
    request_interval_seconds: float = 0.25
    max_retries: int = 3
    retry_backoff_seconds: float = 1.0
    retry_jitter_seconds: float = 0.25
    max_retry_backoff_seconds: float = 30.0


def _load_akshare() -> Any:
    try:
        return importlib.import_module("akshare")
    except ImportError as exc:  # pragma: no cover - exercised in deployment
        raise AkShareUnavailableError(
            "AkShare is not installed; run `uv sync` or install the akshare dependency."
        ) from exc


def _market_for_code(code: str) -> str:
    code = str(code).strip().zfill(6)
    if code.startswith(("4", "8")):
        return "bj"
    return "sh" if code.startswith(("5", "6", "688")) else "sz"


def _numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _fetch_public_flow_frame(symbol: str, market: str) -> pd.DataFrame:
    """Fallback for Eastmoney rejecting AkShare's timestamped request.

    This is the public endpoint used by AkShare's ``stock_individual_fund_flow``
    implementation.  Keeping it here avoids silently changing the data source
    while working around a transient anti-bot response from the wrapper call.
    """
    market_map = {"sh": 1, "sz": 0, "bj": 0}
    if market not in market_map:
        raise AkShareDataError(f"unsupported market: {market}")
    response = httpx.get(
        "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get",
        params={
            "lmt": "0",
            "klt": "101",
            "secid": f"{market_map[market]}.{symbol}",
            "fields1": "f1,f2,f3,f7",
            "fields2": ",".join(f"f{number}" for number in range(51, 66)),
            "ut": "b2884a393a59ad64002292a3e90d46a5",
        },
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    klines = ((payload.get("data") or {}).get("klines") or [])
    if not klines:
        raise AkShareDataError(f"empty public money-flow response for {symbol}")
    columns = [
        "日期",
        "主力净流入-净额",
        "小单净流入-净额",
        "中单净流入-净额",
        "大单净流入-净额",
        "超大单净流入-净额",
        "主力净流入-净占比",
        "小单净流入-净占比",
        "中单净流入-净占比",
        "大单净流入-净占比",
        "超大单净流入-净占比",
        "收盘价",
        "涨跌幅",
        "_unused_1",
        "_unused_2",
    ]
    return pd.DataFrame([row.split(",") for row in klines], columns=columns)


def normalize_flow_frame(frame: pd.DataFrame, *, symbol: str) -> pd.DataFrame:
    """Normalize one ``stock_individual_fund_flow`` response.

    The source names are retained in the mapping below so a schema change does
    not silently turn missing money flow into zero.  Rows with no date or no
    main-flow value are rejected rather than fabricated.
    """
    required = {
        "日期": "trade_date",
        "主力净流入-净额": "main_net_inflow",
        "主力净流入-净占比": "main_inflow_ratio",
        "超大单净流入-净额": "extra_large_net_inflow",
        "大单净流入-净额": "large_net_inflow",
        "大单净流入-净占比": "large_inflow_ratio",
    }
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise AkShareDataError(
            f"AkShare flow response for {symbol} is missing columns: {', '.join(missing)}"
        )

    result = frame.rename(columns=required).copy()
    optional_columns = {"收盘价": "close_price", "涨跌幅": "pct_change"}
    for source_column, target_column in optional_columns.items():
        if source_column in result.columns:
            result = result.rename(columns={source_column: target_column})
        else:
            result[target_column] = None
    result["trade_date"] = pd.to_datetime(result["trade_date"], errors="coerce").dt.date
    for column in (
        "main_net_inflow",
        "main_inflow_ratio",
        "extra_large_net_inflow",
        "large_net_inflow",
        "large_inflow_ratio",
        "close_price",
        "pct_change",
    ):
        result[column] = _numeric(result[column])
    result["symbol"] = str(symbol).zfill(6)
    result = result.dropna(subset=["trade_date", "main_net_inflow"])
    result["calculated_main_net_inflow"] = (
        result["extra_large_net_inflow"] + result["large_net_inflow"]
    )
    result["main_component_gap"] = (
        result["main_net_inflow"] - result["calculated_main_net_inflow"]
    )
    return result[
        [
            "trade_date",
            "symbol",
            "main_net_inflow",
            "main_inflow_ratio",
            "extra_large_net_inflow",
            "large_net_inflow",
            "large_inflow_ratio",
            "calculated_main_net_inflow",
            "main_component_gap",
            "close_price",
            "pct_change",
        ]
    ].sort_values("trade_date")


def summarize_flow_trend(flow: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Return daily breadth, per-stock month summary, and a compact trend report."""
    if flow.empty:
        empty_daily = pd.DataFrame(
            columns=[
                "trade_date", "stock_count", "main_net_inflow_total", "large_net_inflow_total",
                "main_positive_count", "main_positive_ratio", "large_positive_count",
                "large_positive_ratio", "main_net_inflow_median", "large_net_inflow_median",
            ]
        )
        return empty_daily, flow.copy(), {
            "status": "empty",
            "observation_days": 0,
            "stock_count": 0,
        }

    working = flow.copy()
    working["main_positive"] = working["main_net_inflow"] > 0
    working["large_positive"] = working["large_net_inflow"] > 0
    daily = (
        working.groupby("trade_date", as_index=False)
        .agg(
            stock_count=("symbol", "nunique"),
            main_net_inflow_total=("main_net_inflow", "sum"),
            large_net_inflow_total=("large_net_inflow", "sum"),
            main_positive_count=("main_positive", "sum"),
            large_positive_count=("large_positive", "sum"),
            main_net_inflow_median=("main_net_inflow", "median"),
            large_net_inflow_median=("large_net_inflow", "median"),
        )
        .sort_values("trade_date")
    )
    daily["main_positive_ratio"] = daily["main_positive_count"] / daily["stock_count"]
    daily["large_positive_ratio"] = daily["large_positive_count"] / daily["stock_count"]

    stock = (
        working.groupby(["symbol"], as_index=False)
        .agg(
            observation_days=("trade_date", "nunique"),
            main_net_inflow_sum=("main_net_inflow", "sum"),
            large_net_inflow_sum=("large_net_inflow", "sum"),
            main_positive_days=("main_positive", "sum"),
            large_positive_days=("large_positive", "sum"),
            latest_main_net_inflow=("main_net_inflow", "last"),
            latest_large_net_inflow=("large_net_inflow", "last"),
            latest_main_inflow_ratio=("main_inflow_ratio", "last"),
            latest_large_inflow_ratio=("large_inflow_ratio", "last"),
        )
    )
    stock["main_positive_ratio"] = stock["main_positive_days"] / stock["observation_days"]
    stock["large_positive_ratio"] = stock["large_positive_days"] / stock["observation_days"]
    stock = stock.sort_values(
        ["main_net_inflow_sum", "large_net_inflow_sum"], ascending=False
    )

    totals = daily["main_net_inflow_total"].astype(float)
    recent_window = min(3, len(totals))
    recent_mean = float(totals.tail(recent_window).mean())
    early_mean = float(totals.head(recent_window).mean())
    delta = recent_mean - early_mean
    if abs(delta) < max(abs(early_mean) * 0.05, 1.0):
        direction = "震荡"
    elif delta > 0:
        direction = "转强"
    else:
        direction = "转弱"
    summary = {
        "status": "ok",
        "observation_days": int(len(daily)),
        "stock_count": int(working["symbol"].nunique()),
        "date_start": str(daily["trade_date"].min()),
        "date_end": str(daily["trade_date"].max()),
        "main_net_inflow_total": float(working["main_net_inflow"].sum()),
        "large_net_inflow_total": float(working["large_net_inflow"].sum()),
        "main_positive_ratio_mean": float(daily["main_positive_ratio"].mean()),
        "large_positive_ratio_mean": float(daily["large_positive_ratio"].mean()),
        "recent_3d_main_mean": recent_mean,
        "early_3d_main_mean": early_mean,
        "recent_vs_early_delta": float(delta),
        "trend": direction,
    }
    return daily, stock, summary


class AkShareFundFlowProvider:
    """Fetch CSI 300 constituents and their recent daily fund flow."""

    def __init__(
        self,
        *,
        config: AkShareFlowConfig | None = None,
        ak_module: Any | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.config = config or AkShareFlowConfig()
        if self.config.max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if self.config.request_interval_seconds < 0:
            raise ValueError("request_interval_seconds must be >= 0")
        if self.config.retry_backoff_seconds < 0:
            raise ValueError("retry_backoff_seconds must be >= 0")
        if self.config.retry_jitter_seconds < 0:
            raise ValueError("retry_jitter_seconds must be >= 0")
        if self.config.max_retry_backoff_seconds < 0:
            raise ValueError("max_retry_backoff_seconds must be >= 0")
        self.ak = ak_module or _load_akshare()
        self._sleep = sleep
        self._retry_count = 0

    @property
    def retry_count(self) -> int:
        """Number of retries performed by the most recent fetch operation."""
        return self._retry_count

    def _retry_delay(self, attempt: int) -> float:
        """Calculate capped exponential backoff with a small random jitter."""
        base = self.config.retry_backoff_seconds * (2**attempt)
        base = min(base, self.config.max_retry_backoff_seconds)
        jitter = (
            random.uniform(0.0, self.config.retry_jitter_seconds)
            if self.config.retry_jitter_seconds
            else 0.0
        )
        return base + jitter

    def _with_retries(self, operation: Callable[[], Any], *, label: str) -> Any:
        """Run a public-data operation, retrying transient remote failures.

        AkShare delegates to public HTTP endpoints and can fail before an HTTP
        response is available (for example ``RemoteDisconnected``).  Retrying
        the complete operation also covers an empty/malformed response from the
        fallback endpoint.  The final exception is retained as the cause so the
        caller can still diagnose the remote failure.
        """
        last_error: Exception | None = None
        total_attempts = self.config.max_retries + 1
        for attempt in range(total_attempts):
            try:
                return operation()
            except Exception as exc:  # noqa: BLE001 - public wrappers have varied exceptions
                last_error = exc
                if attempt >= self.config.max_retries:
                    break
                delay = self._retry_delay(attempt)
                self._retry_count += 1
                LOGGER.warning(
                    "AkShare request failed label=%s attempt=%d/%d; retrying in %.2fs: %s",
                    label,
                    attempt + 1,
                    total_attempts,
                    delay,
                    exc,
                )
                self._sleep(delay)
        raise AkShareDataError(
            f"failed after {total_attempts} attempts ({label}): {last_error}"
        ) from last_error

    def get_csi300_constituents(self) -> pd.DataFrame:
        """Use the CSI index source first, falling back to Sina's current list."""
        errors: list[str] = []
        for name in ("index_stock_cons_csindex", "index_stock_cons"):
            function = getattr(self.ak, name, None)
            if function is None:
                continue
            try:
                def _fetch_constituents(
                    *,
                    constituent_function: Callable[..., pd.DataFrame] = function,
                    source_name: str = name,
                ) -> pd.DataFrame:
                    frame = constituent_function(symbol="000300")
                    code_column = "成分券代码" if "成分券代码" in frame.columns else "品种代码"
                    name_column = "成分券名称" if "成分券名称" in frame.columns else "品种名称"
                    if code_column not in frame.columns:
                        raise AkShareDataError(
                            f"{source_name} response has no constituent code column"
                        )
                    result = pd.DataFrame(
                        {
                            "symbol": frame[code_column]
                            .astype(str)
                            .str.extract(r"(\d{6})")[0],
                            "name": frame[name_column].astype(str)
                            if name_column in frame
                            else "",
                        }
                    ).dropna(subset=["symbol"])
                    result["symbol"] = result["symbol"].str.zfill(6)
                    result["market"] = result["symbol"].map(_market_for_code)
                    result = (
                        result.drop_duplicates("symbol")
                        .sort_values("symbol")
                        .reset_index(drop=True)
                    )
                    if len(result) < 250:
                        raise AkShareDataError(
                            f"{source_name} returned only {len(result)} constituents"
                        )
                    return result

                return self._with_retries(_fetch_constituents, label=name)
            except Exception as exc:  # noqa: BLE001 - fallback must preserve cause
                errors.append(f"{name}: {exc}")
        raise AkShareDataError("Unable to fetch CSI 300 constituents; " + " | ".join(errors))

    def _fetch_one(self, symbol: str, market: str) -> pd.DataFrame:
        function = getattr(self.ak, "stock_individual_fund_flow", None)
        if function is None:
            raise AkShareUnavailableError(
                "Installed AkShare has no stock_individual_fund_flow function"
            )

        def _fetch_once() -> pd.DataFrame:
            try:
                frame = function(stock=symbol, market=market)
                return normalize_flow_frame(frame, symbol=symbol)
            except Exception as exc:  # noqa: BLE001 - retry public endpoint failures
                try:
                    frame = _fetch_public_flow_frame(symbol, market)
                    return normalize_flow_frame(frame, symbol=symbol)
                except Exception as fallback_exc:  # noqa: BLE001 - preserve both causes
                    raise AkShareDataError(
                        f"wrapper={exc}; public_fallback={fallback_exc}"
                    ) from fallback_exc

        return self._with_retries(_fetch_once, label=f"flow:{symbol}")

    def fetch_symbol_frame(self, *, symbol: str, market: str) -> pd.DataFrame:
        """Fetch a normalized, unaggregated frame for database ingestion."""
        normalized_symbol = str(symbol).strip().zfill(6)
        self._retry_count = 0
        return self._fetch_one(normalized_symbol, market)

    def fetch_etf_universe(
        self,
        *,
        min_market_cap: float = 10_000_000_000,
        max_symbols: int | None = None,
    ) -> pd.DataFrame:
        """Return ETFs whose current total market value passes the threshold.

        ``fund_etf_spot_em`` exposes ``总市值`` in yuan.  This is a universe
        discovery snapshot, not historical AUM, and should be refreshed before
        a scheduled batch rather than treated as a backtest point-in-time fact.
        """
        if min_market_cap < 0:
            raise ValueError("min_market_cap must be >= 0")
        if max_symbols is not None and max_symbols < 1:
            raise ValueError("max_symbols must be >= 1 when provided")
        function = getattr(self.ak, "fund_etf_spot_em", None)
        if function is None:
            raise AkShareUnavailableError(
                "Installed AkShare has no fund_etf_spot_em function"
            )

        self._retry_count = 0
        frame = self._with_retries(function, label="etf:spot")
        required = {"代码", "名称", "总市值"}
        missing = sorted(required.difference(frame.columns))
        if missing:
            raise AkShareDataError(
                "ETF spot response is missing columns: " + ", ".join(missing)
            )
        result = pd.DataFrame(
            {
                "symbol": frame["代码"].astype(str).str.extract(r"(\d{6})")[0],
                "name": frame["名称"].astype(str).str.strip(),
                "total_market_cap": pd.to_numeric(frame["总市值"], errors="coerce"),
            }
        )
        result["market"] = result["symbol"].map(_market_for_code)
        result = result.dropna(subset=["symbol", "total_market_cap"])
        result = result[result["total_market_cap"] >= min_market_cap]
        result = (
            result.drop_duplicates("symbol")
            .sort_values("total_market_cap", ascending=False)
            .reset_index(drop=True)
        )
        if max_symbols is not None:
            result = result.head(max_symbols).copy()
        return result[["symbol", "name", "market", "total_market_cap"]]

    def fetch_month(
        self,
        *,
        start_date: date,
        end_date: date,
        max_stocks: int | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
        self._retry_count = 0
        constituents = self.get_csi300_constituents()
        if max_stocks is not None:
            constituents = constituents.head(max_stocks)
        rows: list[pd.DataFrame] = []
        failures: list[dict[str, str]] = []
        for item in constituents.itertuples(index=False):
            try:
                frame = self._fetch_one(item.symbol, item.market)
                frame = frame[
                    (frame["trade_date"] >= start_date) & (frame["trade_date"] <= end_date)
                ]
                if not frame.empty:
                    rows.append(frame)
            except Exception as exc:  # noqa: BLE001 - continue across the index
                LOGGER.warning("AkShare flow fetch failed for %s: %s", item.symbol, exc)
                failures.append({"symbol": item.symbol, "error": str(exc)})
            self._sleep(self.config.request_interval_seconds)
        flow = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
        daily, stock, summary = summarize_flow_trend(flow)
        summary.update(
            {
                "requested_stock_count": int(len(constituents)),
                "successful_stock_count": int(flow["symbol"].nunique()) if not flow.empty else 0,
                "failed_stock_count": len(failures),
                "retry_count": self._retry_count,
                "failures": failures,
                "source": "akshare",
                "source_note": (
                    "AkShare public wrappers; underlying endpoint availability "
                    "and rate limits may change"
                ),
            }
        )
        return daily, stock, summary

    def fetch_symbol(
        self,
        *,
        symbol: str,
        market: str,
        start_date: date,
        end_date: date,
    ) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
        """Fetch one stock or ETF, useful for connectivity and schema probes."""
        flow = self.fetch_symbol_frame(symbol=symbol, market=market)
        flow = flow[
            (flow["trade_date"] >= start_date) & (flow["trade_date"] <= end_date)
        ]
        daily, stock, summary = summarize_flow_trend(flow)
        summary.update(
            {
                "requested_stock_count": 1,
                "successful_stock_count": int(not flow.empty),
                "failed_stock_count": int(flow.empty),
                "retry_count": self._retry_count,
                "source": "akshare",
                "source_note": (
                    "AkShare public wrappers; underlying endpoint availability "
                    "and rate limits may change"
                ),
            }
        )
        return daily, stock, summary


def month_bounds(today: date | None = None) -> tuple[date, date]:
    current = today or date.today()
    return current.replace(day=1), current
