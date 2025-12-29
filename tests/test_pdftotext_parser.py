import logging
from pathlib import Path

import pandas as pd
import pytest

from missouri_vsr.assets import extract as extract_mod


def _parse_fixture(name: str, year: int):
    path = Path("text-experiments") / name
    if not path.exists():
        pytest.skip(f"Missing fixture: {path}")
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    log = logging.getLogger("pdftotext_test")
    log.setLevel(logging.CRITICAL)
    return extract_mod._parse_pdftotext_lines(lines, log, year=year, pdf_name=name)


def test_front_matter_is_ignored_until_agency():
    df = _parse_fixture("2020_p1_16.txt", year=2020)
    assert not df.empty
    assert (df["agency"].str.contains("Adair County Sheriff", na=False)).any()
    assert (df["table_id"] == "rates-by-race").any()
    assert (df["row_key"] == "rates-by-race--totals--all-stops").any()


def test_parses_rates_table_rows():
    df = _parse_fixture("2023_p11_16.txt", year=2023)
    assert not df.empty

    row = df[
        (df["table"] == "Rates by Race")
        & (df["metric"].str.lower() == "all stops")
    ].iloc[0]

    assert row["table_id"] == "rates-by-race"
    assert row["section"] == "Totals"
    assert row["row_key"] == "rates-by-race--totals--all-stops"
    assert row["metric_id"] == "all-stops"
    assert row["row_id"] == "2023-adair-county-sheriff-s-dept-rates-by-race--totals--all-stops"
    assert row["Total"] == pytest.approx(317.0)


def test_parses_stops_table_sections():
    df = _parse_fixture("2023_p11_16.txt", year=2023)
    row = df[
        (df["table"] == "Number of Stops by Race")
        & (df["metric"].str.lower() == "moving")
    ].iloc[0]

    assert row["table_id"] == "number-of-stops-by-race"
    assert row["section"] == "Reason for Stop"
    assert row["row_key"] == "number-of-stops-by-race--reason-for-stop--moving"
    assert row["Total"] == pytest.approx(173.0)


def test_parses_disparity_index_table():
    df = _parse_fixture("2020_p11_16.txt", year=2020)
    row = df[
        (df["table"] == "Disparity Index by Race")
        & (df["section"] == "Disparity index")
        & (df["metric"].str.lower() == "all stops")
    ].iloc[0]

    assert row["table_id"] == "disparity-index-by-race"
    assert row["row_key"] == "disparity-index-by-race--disparity-index--all-stops"
    assert pd.isna(row["Total"])
    assert row["White"] == pytest.approx(0.906, rel=1e-3)


def test_parses_columnar_layout_table():
    lines = [
        "Agency Results",
        "2.1  Adair County Sheriff's Dept",
        "Table 1: Rates by Race for Adair County Sheriff's Dept",
        "Population",
        "2022 ACS pop.",
        "Totals",
        "All stops",
        "Total",
        "10",
        "20",
        "White",
        "30",
        "40",
        "Black",
        "50",
        "60",
        "Hispanic",
        "70",
        "80",
        "Native American",
        "90",
        "100",
        "Asian",
        "110",
        "120",
        "Other",
        "130",
        "140",
    ]
    log = logging.getLogger("pdftotext_test")
    log.setLevel(logging.CRITICAL)
    df = extract_mod._parse_pdftotext_lines(lines, log, year=2023, pdf_name="columnar_sample.txt")

    row = df[(df["metric"] == "All stops")].iloc[0]
    assert row["section"] == "Totals"
    assert row["Total"] == pytest.approx(20.0)
    assert row["White"] == pytest.approx(40.0)
    assert row["Black"] == pytest.approx(60.0)
    assert row["Other"] == pytest.approx(140.0)
