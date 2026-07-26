"""External macro/positioning data normalized with point-in-time availability."""
from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
from io import StringIO
from typing import Any, Protocol

import httpx

from vntdr.models import FactorObservation, Instrument


class ExternalFactorProvider(Protocol):
    def fetch(self, *, instrument: Instrument, start: datetime, end: datetime) -> list[FactorObservation]: ...


class FredCsvProvider:
    """FRED graph CSV reader; delayed availability avoids assuming same-day release."""

    def __init__(self, *, series_id: str, factor_name: str, availability_delay: timedelta = timedelta(days=1), client: httpx.Client | None = None) -> None:
        self.series_id = series_id
        self.factor_name = factor_name
        self.availability_delay = availability_delay
        self.client = client or httpx.Client(timeout=20)

    def fetch(self, *, instrument: Instrument, start: datetime, end: datetime) -> list[FactorObservation]:
        response = self.client.get("https://fred.stlouisfed.org/graph/fredgraph.csv", params={"id": self.series_id})
        response.raise_for_status()
        observations: list[FactorObservation] = []
        for row in csv.DictReader(StringIO(response.text)):
            raw_value = row.get(self.series_id)
            if raw_value in {None, ".", ""}:
                continue
            observed_at = datetime.fromisoformat(row["DATE"]).replace(tzinfo=timezone.utc)
            if start <= observed_at <= end:
                observations.append(FactorObservation(
                    instrument=instrument, factor_name=self.factor_name, value=float(raw_value),
                    observed_at=observed_at, available_at=observed_at + self.availability_delay,
                    metadata={"source": "FRED", "series_id": self.series_id},
                ))
        return observations


class CftcPositioningProvider:
    """Normalize CFTC-style weekly rows into a net speculative positioning factor.

    Fetching is injected because CFTC report endpoints and schemas vary by report
    family; every input row must expose report_date, long and short values.
    """

    def __init__(self, *, factor_name: str = "cftc_net_positioning", availability_delay: timedelta = timedelta(days=3)) -> None:
        self.factor_name = factor_name
        self.availability_delay = availability_delay

    def normalize(self, *, instrument: Instrument, rows: list[dict[str, Any]], start: datetime, end: datetime) -> list[FactorObservation]:
        observations = []
        for row in rows:
            report_date = row["report_date"]
            if isinstance(report_date, str):
                report_date = datetime.fromisoformat(report_date).replace(tzinfo=timezone.utc)
            if report_date.tzinfo is None:
                report_date = report_date.replace(tzinfo=timezone.utc)
            if not start <= report_date <= end:
                continue
            long, short = float(row["long"]), float(row["short"])
            denominator = long + short
            observations.append(FactorObservation(
                instrument=instrument, factor_name=self.factor_name,
                value=(long - short) / denominator if denominator else 0.0,
                observed_at=report_date, available_at=report_date + self.availability_delay,
                metadata={"source": "CFTC", "long": long, "short": short},
            ))
        return observations


class OkxDerivativesProvider:
    """Public OKX funding/open-interest observations for derivative confirmation.

    Funding history is timestamped by its realised funding event. Open interest
    is a point-in-time snapshot and is therefore only emitted at the exchange
    timestamp returned by the API. Neither series is backfilled from a later
    observation into an earlier decision.
    """

    def __init__(self, *, base_url: str = "https://www.okx.com", client: httpx.Client | None = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.client = client or httpx.Client(timeout=20)

    @staticmethod
    def _timestamp(value: str | int | float) -> datetime:
        return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)

    def fetch(self, *, instrument: Instrument, start: datetime, end: datetime) -> list[FactorObservation]:
        funding_response = self.client.get(
            f"{self.base_url}/api/v5/public/funding-rate-history",
            params={"instId": instrument.symbol, "limit": "100"},
        )
        funding_response.raise_for_status()
        payload = funding_response.json()
        if payload.get("code", "0") != "0":
            raise RuntimeError(f"OKX funding API error: {payload}")
        observations: list[FactorObservation] = []
        for row in payload.get("data", []):
            timestamp = row.get("fundingTime") or row.get("ts")
            if timestamp is None:
                continue
            observed_at = self._timestamp(timestamp)
            if start <= observed_at <= end:
                rate = float(row.get("realizedRate", row.get("fundingRate", 0.0)))
                observations.append(FactorObservation(
                    instrument=instrument, factor_name="okx_funding_rate", value=rate,
                    observed_at=observed_at, available_at=observed_at,
                    metadata={"source": "OKX", "inst_id": instrument.symbol},
                ))
        open_interest_response = self.client.get(
            f"{self.base_url}/api/v5/public/open-interest",
            params={"instId": instrument.symbol},
        )
        open_interest_response.raise_for_status()
        open_interest_payload = open_interest_response.json()
        if open_interest_payload.get("code", "0") != "0":
            raise RuntimeError(f"OKX open-interest API error: {open_interest_payload}")
        for row in open_interest_payload.get("data", []):
            timestamp = row.get("ts")
            if timestamp is None:
                continue
            observed_at = self._timestamp(timestamp)
            # A live snapshot can be generated milliseconds after the caller
            # captured ``end``. Keep this narrow tolerance; its own
            # ``available_at`` still prevents it from being used earlier.
            if start <= observed_at <= end + timedelta(minutes=5):
                observations.append(FactorObservation(
                    instrument=instrument, factor_name="okx_open_interest",
                    value=float(row.get("oi", 0.0)), observed_at=observed_at,
                    available_at=observed_at,
                    metadata={"source": "OKX", "inst_id": instrument.symbol, "oi_ccy": row.get("oiCcy")},
                ))
        return sorted(observations, key=lambda item: (item.observed_at, item.factor_name))
