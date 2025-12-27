import json
import os

import pandas as pd
import pytest
from dagster import build_op_context

from missouri_vsr.assets import agency_reference as agency_mod
from missouri_vsr.resources import LocalDirectoryResource


class FakeResponse:
    def __init__(self, status_code: int, payload: dict | None):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = json.dumps(self._payload)

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            request = agency_mod.httpx.Request("GET", "https://api.geocod.io/v1.9/geocode")
            response = agency_mod.httpx.Response(self.status_code, request=request, text=self.text)
            raise agency_mod.httpx.HTTPStatusError("HTTP error", request=request, response=response)


class FakeClient:
    def __init__(self, responses_by_query: dict[str, FakeResponse], calls: list[str] | None = None):
        self._responses = responses_by_query
        self._calls = calls

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, url, params=None):
        query = (params or {}).get("q")
        if self._calls is not None:
            self._calls.append(query)
        return self._responses.get(query, FakeResponse(200, {"results": []}))


def _context(tmp_path):
    return build_op_context(
        resources={"data_dir_processed": LocalDirectoryResource(path=str(tmp_path))}
    )


def test_geocode_county_flow(monkeypatch, tmp_path):
    city_query = "Columbia, MO"
    county_query = "Boone County, MO"

    city_payload = {
        "results": [
            {"address_components": {"city": "Columbia"}},
            {"address_components": {"city": "Columbia", "county": "Boone County"}},
        ]
    }
    county_payload = {
        "results": [
            {"address_components": {"county": "Boone County"}, "fields": {}},
            {
                "address_components": {"county": "Boone County"},
                "fields": {"cd": [{"name": "MO 1"}]},
            },
        ]
    }

    responses = {
        city_query: FakeResponse(200, city_payload),
        county_query: FakeResponse(200, county_payload),
    }

    monkeypatch.setenv("GEOCODIO_API_KEY", "test-key")
    monkeypatch.setattr(agency_mod.httpx, "Client", lambda timeout=None: FakeClient(responses))

    df = pd.DataFrame(
        [
            {
                "Department": "Boone County Sheriff",
                "AgencyType": "County",
                "AddressCity": "Columbia",
            }
        ]
    )
    out = agency_mod.geocode_agency_reference(_context(tmp_path), df)
    row = out.iloc[0]

    assert row["geocode_jurisdiction_scope"] == "county"
    assert row["geocode_jurisdiction_status"] == "ok"
    assert row["geocode_jurisdiction_query"] == county_query
    assert row["geocode_jurisdiction_selected_index"] == 1
    assert row["geocode_jurisdiction_county"] == "Boone County"
    assert json.loads(row["geocode_jurisdiction_response"])["results"]
    assert row["geocode_address_status"] == "ok"
    assert row["geocode_address_query"] == city_query
    assert json.loads(row["geocode_address_response"])["results"]


def test_geocode_municipal_selects_fields(monkeypatch, tmp_path):
    city_query = "St. Louis, MO"
    payload = {
        "results": [
            {"address_components": {"city": "St. Louis", "county": "St. Louis County"}, "fields": {}},
            {
                "address_components": {"city": "St. Louis", "county": "St. Louis County"},
                "fields": {"cd": [{"name": "MO 1"}]},
            },
        ]
    }
    responses = {city_query: FakeResponse(200, payload)}

    monkeypatch.setenv("GEOCODIO_API_KEY", "test-key")
    monkeypatch.setattr(agency_mod.httpx, "Client", lambda timeout=None: FakeClient(responses))

    df = pd.DataFrame(
        [
            {
                "Department": "St. Louis Police Department",
                "AgencyType": "municipal",
                "AddressCity": "St. Louis",
            }
        ]
    )
    out = agency_mod.geocode_agency_reference(_context(tmp_path), df)
    row = out.iloc[0]

    assert row["geocode_jurisdiction_scope"] == "municipal"
    assert row["geocode_jurisdiction_status"] == "ok"
    assert row["geocode_jurisdiction_query"] == city_query
    assert row["geocode_jurisdiction_selected_index"] == 1
    assert row["geocode_jurisdiction_county"] == "St. Louis County"
    assert row["geocode_address_status"] == "ok"


def test_geocode_address_type(monkeypatch, tmp_path):
    query = "123 Runway Rd, Suite 2, Kansas City, 64101, MO"
    payload = {
        "results": [
            {
                "address_components": {"county": "Jackson County"},
                "fields": {"cd": [{"name": "MO 5"}]},
            }
        ]
    }
    responses = {query: FakeResponse(200, payload)}

    df = pd.DataFrame(
        [
            {
                "Department": "Metro Airport Police",
                "AgencyType": "Airport",
                "AddressLine1": "123 Runway Rd",
                "AddressLine2": "Suite 2",
                "AddressCity": "Kansas City",
                "AddressZip": "64101",
            }
        ]
    )
    monkeypatch.setenv("GEOCODIO_API_KEY", "test-key")
    monkeypatch.setattr(agency_mod.httpx, "Client", lambda timeout=None: FakeClient(responses))

    out = agency_mod.geocode_agency_reference(_context(tmp_path), df)
    row = out.iloc[0]

    assert row["geocode_jurisdiction_scope"] == "address"
    assert row["geocode_jurisdiction_status"] == "skipped"
    assert row["geocode_jurisdiction_reason"] == "no_jurisdiction"
    assert row["geocode_address_status"] == "ok"
    assert row["geocode_address_query"] == query
    assert row["geocode_address_county"] == "Jackson County"
    assert json.loads(row["geocode_address_response"])["results"]


def test_geocode_missing_city(monkeypatch, tmp_path):
    monkeypatch.setenv("GEOCODIO_API_KEY", "test-key")
    monkeypatch.setattr(agency_mod.httpx, "Client", lambda timeout=None: FakeClient({}))

    df = pd.DataFrame(
        [
            {
                "Department": "Missing City PD",
                "AgencyType": "municipal",
                "AddressCity": None,
            }
        ]
    )
    out = agency_mod.geocode_agency_reference(_context(tmp_path), df)
    row = out.iloc[0]

    assert row["geocode_address_status"] == "skipped"
    assert row["geocode_address_reason"] == "missing_address"
    assert row["geocode_jurisdiction_status"] == "skipped"
    assert row["geocode_jurisdiction_reason"] == "missing_city"


def test_geocode_missing_api_key(tmp_path, monkeypatch):
    monkeypatch.delenv("GEOCODIO_API_KEY", raising=False)

    df = pd.DataFrame(
        [
            {
                "Department": "No Key PD",
                "AgencyType": "municipal",
                "AddressCity": "Columbia",
            }
        ]
    )
    out = agency_mod.geocode_agency_reference(_context(tmp_path), df)
    row = out.iloc[0]

    assert row["geocode_address_status"] == "missing_api_key"
    assert row["geocode_jurisdiction_status"] == "missing_api_key"


def test_geocode_state_agency_cached(monkeypatch, tmp_path):
    state_payload = {
        "results": [
            {
                "address_components": {"state": "MO", "county": "Boone County"},
                "fields": {"cd": [{"name": "MO 1"}]},
            }
        ]
    }
    calls: list[str] = []

    monkeypatch.setenv("GEOCODIO_API_KEY", "test-key")
    monkeypatch.setattr(
        agency_mod.httpx,
        "Client",
        lambda timeout=None: FakeClient({"Missouri": FakeResponse(200, state_payload)}, calls),
    )

    df = pd.DataFrame(
        [
            {"Department": "State Agency One", "AgencyType": "state agency"},
            {"Department": "State Agency Two", "AgencyType": "state agency"},
        ]
    )
    out = agency_mod.geocode_agency_reference(_context(tmp_path), df)

    assert calls.count("Missouri") == 1
    assert (out["geocode_jurisdiction_scope"] == "state").all()
    assert (out["geocode_jurisdiction_query"] == "Missouri").all()
    assert (out["geocode_jurisdiction_status"] == "ok").all()
    assert (out["geocode_jurisdiction_county"] == "Boone County").all()


@pytest.mark.network
def test_geocode_live_smoke(tmp_path):
    if not os.getenv("GEOCODIO_LIVE") or not os.getenv("GEOCODIO_API_KEY"):
        pytest.skip("Set GEOCODIO_LIVE=1 and GEOCODIO_API_KEY to run live geocoding test.")

    df = pd.DataFrame(
        [
            {
                "Department": "Live Test PD",
                "AgencyType": "municipal",
                "AddressCity": "Columbia",
            }
        ]
    )
    out = agency_mod.geocode_agency_reference(_context(tmp_path), df)
    row = out.iloc[0]

    assert row["geocode_jurisdiction_status"] in {"ok", "error", "skipped"}
    if row["geocode_jurisdiction_response"]:
        payload = json.loads(row["geocode_jurisdiction_response"])
        assert "results" in payload
