import csv

import numpy as np
import pandas as pd
from dagster import AssetCheckResult, MetadataValue, asset_check

from missouri_vsr.assets.reports import combine_all_reports

EXPECTED_COLUMNS = [
    "Key",
    "Total",
    "White",
    "Black",
    "Hispanic",
    "Native American",
    "Asian",
    "Other",
    "year",
    "slug",
    "Department",
    "Table name",
    "Measurement",
    "agency",
    "table",
    "table_id",
    "section",
    "metric",
    "metric_id",
    "row_id",
]

NUMERIC_COLS = [
    "Total",
    "White",
    "Black",
    "Hispanic",
    "Native American",
    "Asian",
    "Other",
]  # Just the race-specific numbers


def _coerce_row(raw: dict) -> dict:
    """
    Convert the string values coming from csv.DictReader to the
    types used in the DataFrame so comparisons line up.
    """
    out = {}
    for k, v in raw.items():
        if v == "":            # empty cell → treat as missing
            out[k] = None
        elif k == "year" and v.isdigit():
            out[k] = int(v)
        elif k in NUMERIC_COLS:
            # keep integers as int, everything else as float
            out[k] = int(v) if v.isdigit() else float(v)
        else:
            out[k] = v
    return out


RENAME_CHECK_FIELDS = {
    "key": "Key",
    "department": "Department",
    "table name": "Table name",
    "section": "Measurement",
}


def _rename_check_fields(d: dict) -> dict:
    return {RENAME_CHECK_FIELDS.get(k, k): v for k, v in d.items()}


with open("data_checks/row_sanity_checks.csv") as f:
    ROW_EXPECTATIONS = [_rename_check_fields(_coerce_row(r)) for r in csv.DictReader(f)]


# Schema check for extracted pdf data – ensure *exact* match with `EXPECTED_COLUMNS`.
@asset_check(asset=combine_all_reports)
def check_expected_columns(df: pd.DataFrame) -> AssetCheckResult:
    missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    extra = [c for c in df.columns if c not in EXPECTED_COLUMNS]

    passed = not missing and not extra
    return AssetCheckResult(
        passed=passed,
        metadata={
            "missing_columns": missing,
            "extra_columns": extra,
        },
    )


# Duplicate‐slug check – combination of dept/slug/year must be unique.
@asset_check(asset=combine_all_reports)
def check_no_duplicate_slugs(df: pd.DataFrame) -> AssetCheckResult:
    dupes = df[df.duplicated(["Department", "slug", "year"], keep=False)]
    passed = dupes.empty

    # Include both department and slug for each duplicate row
    example_duplicates = dupes[["Department", "slug", "year"]].drop_duplicates().to_dict(orient="records")

    return AssetCheckResult(
        passed=passed,
        metadata={
            "duplicate_count": len(dupes),
            "example_duplicates": example_duplicates,
        },
    )


# Duplicate row_id check – row_id + year must be unique.
@asset_check(asset=combine_all_reports)
def check_no_duplicate_row_ids(df: pd.DataFrame) -> AssetCheckResult:
    if "row_id" not in df.columns or "year" not in df.columns:
        return AssetCheckResult(
            passed=False,
            metadata={"reason": "row_id/year columns missing"},
        )

    dupes = df[df.duplicated(["row_id", "year"], keep=False)]
    passed = dupes.empty
    example_duplicates = (
        dupes[["row_id", "year", "Department", "slug"]]
        .drop_duplicates()
        .head(25)
        .to_dict(orient="records")
    )

    return AssetCheckResult(
        passed=passed,
        metadata={
            "duplicate_count": len(dupes),
            "example_duplicates": example_duplicates,
        },
    )


# Verify every numeric cell parses as a number or is NaN.
@asset_check(asset=combine_all_reports)
def check_numeric_columns_parse(df: pd.DataFrame) -> AssetCheckResult:
    numeric_parsed = df[NUMERIC_COLS].apply(lambda s: pd.to_numeric(s, errors="coerce"))

    non_numeric_mask = numeric_parsed.isna() & df[NUMERIC_COLS].notna()
    problem_cells = non_numeric_mask.stack()

    passed = not problem_cells.any()
    return AssetCheckResult(
        passed=passed,
        metadata={
            "non_numeric_cells": int(problem_cells.sum()),
        },
    )


def _convert_types(obj):
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, dict):
        return {k: _convert_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_convert_types(item) for item in obj]
    else:
        return obj


def _make_year_expectation_check(asset, year: int, rows: list[dict]):
    """Return an `asset_check` enforcing that all rows for a year match."""

    check_name = f"row_expectations_{year}"

    @asset_check(name=check_name, asset=asset)
    def _check(df: pd.DataFrame) -> AssetCheckResult:
        year_df = df[df["year"] == year].copy()
        if year_df.empty:
            return AssetCheckResult(
                passed=False,
                metadata={"reason": f"no rows found for year {year}"},
            )

        year_df = year_df.drop_duplicates(["slug", "Department", "year"])
        year_index = year_df.set_index(["slug", "Department", "year"], drop=False)

        missing = []
        mismatches = []
        for check in rows:
            key = (check.get("slug"), check.get("Department"), check.get("year"))
            if key not in year_index.index:
                missing.append({"slug": key[0], "Department": key[1], "year": key[2]})
                continue

            actual_row = year_index.loc[key]
            if isinstance(actual_row, pd.DataFrame):
                actual_row = actual_row.iloc[0]

            row_mismatches = []
            for field, expected in check.items():
                if field in ("slug", "Department", "checked"):
                    continue

                actual_val = actual_row.get(field)

                # Treat both None and NaN as equivalent
                if pd.isna(actual_val) and expected is None:
                    continue

                # Otherwise, check for (fairly) strict equality
                if actual_val != expected:
                    row_mismatches.append(
                        {"field": field, "expected": expected, "actual": actual_val}
                    )

            if row_mismatches:
                mismatches.append(
                    {
                        "slug": key[0],
                        "Department": key[1],
                        "year": key[2],
                        "mismatches": row_mismatches,
                    }
                )

        sample_limit = 20
        missing_sample = _convert_types(missing[:sample_limit])
        mismatches_sample = _convert_types(mismatches[:sample_limit])

        passed = not missing and not mismatches
        return AssetCheckResult(
            passed=passed,
            metadata={
                "year": year,
                "expected_rows": len(rows),
                "missing_count": len(missing),
                "mismatch_count": len(mismatches),
                "missing_sample": MetadataValue.json(missing_sample),
                "mismatch_sample": MetadataValue.json(mismatches_sample),
            },
        )

    return _check


ROWS_BY_YEAR: dict[int, list[dict]] = {}
for check in ROW_EXPECTATIONS:
    year = check.get("year")
    if isinstance(year, int):
        ROWS_BY_YEAR.setdefault(year, []).append(check)
    elif isinstance(year, str) and year.isdigit():
        ROWS_BY_YEAR.setdefault(int(year), []).append(check)


year_expectation_checks = [
    _make_year_expectation_check(combine_all_reports, year, rows)
    for year, rows in sorted(ROWS_BY_YEAR.items())
]

# Aggregate if you need to expose the list for Dagster’s loader utilities
asset_checks = [
    check_expected_columns,
    check_no_duplicate_slugs,
    check_no_duplicate_row_ids,
    check_numeric_columns_parse,
    *year_expectation_checks,
]
