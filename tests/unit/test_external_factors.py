from __future__ import annotations

from datetime import datetime, timedelta, timezone

from vntdr.models import Instrument
from vntdr.services.external_factors import CftcPositioningProvider, FredCsvProvider, OkxDerivativesProvider


class Response:
    text = "DATE,DFF\n2026-01-01,4.5\n2026-01-02,.\n"

    def raise_for_status(self):
        return None


class Client:
    def get(self, url, params):
        assert params == {"id": "DFF"}
        return Response()


def test_fred_provider_assigns_delayed_availability() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    values = FredCsvProvider(series_id="DFF", factor_name="policy_rate", client=Client()).fetch(
        instrument=Instrument(symbol="XAU-USDT-SWAP", exchange="OKX", asset_class="commodity"), start=start, end=start + timedelta(days=2)
    )
    assert len(values) == 1
    assert values[0].value == 4.5
    assert values[0].available_at == start + timedelta(days=1)


def test_cftc_provider_normalizes_net_position_and_delays_release() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    values = CftcPositioningProvider().normalize(
        instrument=Instrument(symbol="XAU-USDT-SWAP", exchange="OKX", asset_class="commodity"),
        rows=[{"report_date": "2026-01-01", "long": 70, "short": 30}], start=start, end=start,
    )
    assert values[0].value == 0.4
    assert values[0].available_at == start + timedelta(days=3)


class JsonResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class OkxClient:
    def get(self, url, params):
        if url.endswith("funding-rate-history"):
            assert params == {"instId": "BTC-USDT-SWAP", "limit": "100"}
            return JsonResponse({"code": "0", "data": [{"fundingTime": "1767225600000", "realizedRate": "0.0001"}]})
        assert url.endswith("open-interest")
        return JsonResponse({"code": "0", "data": [{"ts": "1767225600000", "oi": "123", "oiCcy": "1"}]})


def test_okx_derivatives_provider_normalizes_funding_and_open_interest() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    values = OkxDerivativesProvider(client=OkxClient()).fetch(
        instrument=Instrument(symbol="BTC-USDT-SWAP", exchange="OKX"),
        start=start,
        end=start + timedelta(days=1),
    )
    assert [(value.factor_name, value.value) for value in values] == [
        ("okx_funding_rate", 0.0001),
        ("okx_open_interest", 123.0),
    ]
    assert all(value.available_at == start for value in values)
