import pandas as pd

from missouri_vsr.asset_checks.asset_checks import check_relationship_columns
from missouri_vsr.assets.gis import (
    _pick_agency_id,
    normalize_county_geoid,
    normalize_place_geoid,
)


def test_relationship_check_metadata_bool():
    df = pd.DataFrame(
        {
            "agency_id": ["agency-1"],
            "touching_ids": [[]],
            "contained_ids": [[]],
        }
    )
    result = check_relationship_columns(df)
    def _unwrap(value):
        return value.value if hasattr(value, "value") else value
    assert result.passed is True
    assert _unwrap(result.metadata["touching_ok"]) is True
    assert _unwrap(result.metadata["contained_ok"]) is True
    assert isinstance(_unwrap(result.metadata["touching_ok"]), bool)
    assert isinstance(_unwrap(result.metadata["contained_ok"]), bool)


def test_pick_agency_id_handles_na():
    row = pd.Series(
        {
            "Department": pd.NA,
            "Canonical": pd.NA,
            "AgencyName": "Example Agency",
        }
    )
    assert _pick_agency_id(row) == "example-agency"

    row = pd.Series({"agency_id": "agency-xyz"})
    assert _pick_agency_id(row) == "agency-xyz"


def test_normalize_geoids():
    assert normalize_county_geoid("29", "003") == "29003"
    assert normalize_county_geoid("29", "29003") == "29003"
    assert normalize_county_geoid("29", "abc") is None

    assert normalize_place_geoid("29", "12345") == "2912345"
    assert normalize_place_geoid("29", "2912345") == "2912345"
    assert normalize_place_geoid("29", "") is None
