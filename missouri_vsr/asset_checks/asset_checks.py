import csv

import numpy as np
import pandas as pd
from slugify import slugify
from dagster import AssetCheckResult, AssetCheckSpec, MetadataValue, asset_check

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
    ROW_SANITY_CHECKS = [_rename_check_fields(_coerce_row(r)) for r in csv.DictReader(f)]


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


def _make_row_sanity_check(asset, check: dict, idx: int) -> AssetCheckSpec:
    """Return an `asset_check` enforcing that *one* row matches `check`."""

    check_slug = check["slug"].replace("-", "_")
    dept_slug = slugify(check["Department"], separator="_")
    check_name = f"sanity_check_{idx}_{check_slug}_{dept_slug}_{check['year']}"

    @asset_check(name=check_name, asset=asset)
    def _check(df: pd.DataFrame) -> AssetCheckResult:
        # Ensure both conditions are applied correctly with parentheses
        row = df[(df["slug"] == check["slug"]) & (df["Department"] == check["Department"]) & (df["year"] == check["year"])]
        if row.empty:
            return AssetCheckResult(
                passed=False,
                metadata={"reason": f"{check['slug']} + {check['Department']} + {check['year']} not found"},
            )

        # Check all additional fields in the dict (besides slug/department)
        mismatches = []
        for key, val in check.items():
            if key in ("slug", "Department"):
                continue

            actual_val = row.iloc[0][key]

            # Treat both None and NaN as equivalent
            if pd.isna(actual_val) and val is None:
                continue

            # Otherwise, check for (fairly) strict equality
            if actual_val != val:
                mismatches.append({"field": key, "expected": val, "actual": actual_val})

        mismatches = _convert_types(mismatches)

        return AssetCheckResult(
            passed=not mismatches,
            metadata={
                "checked_fields": [k for k in check.keys() if k not in ("slug", "Department")],
                "mismatches": MetadataValue.json(mismatches),
            },
        )

    return _check


# Dynamically materialise any row-level checks defined above.
row_sanity_checks = [
    _make_row_sanity_check(combine_all_reports, check, i)
    for i, check in enumerate(ROW_SANITY_CHECKS)
]

# Aggregate if you need to expose the list for Dagster’s loader utilities
asset_checks = [
    check_expected_columns,
    check_no_duplicate_slugs,
    check_numeric_columns_parse,
    *row_sanity_checks,
]
