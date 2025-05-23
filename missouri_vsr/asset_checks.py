"""
Asset checks for the `extract_pdf_data` graph asset.
"""

import pandas as pd
from dagster import AssetCheckResult, AssetCheckSpec, asset_check

# ---------------------------------------------------------------------------
# Import the asset under test.
# ---------------------------------------------------------------------------
# ⚠️  Update this import path to wherever your `extract_pdf_data` asset lives.
from missouri_vsr.assets import extract_pdf_data  # noqa: F401,E501

# ---------------------------------------------------------------------------
# Constants – expected schema
# ---------------------------------------------------------------------------
EXPECTED_COLUMNS = [
    "key",
    "Total",
    "White",
    "Black",
    "Hispanic",
    "Native American",
    "Asian",
    "Other",
    "department",
    "table name",
]

NUMERIC_COLS = EXPECTED_COLUMNS[1:8]  # race‐specific totals only

# ---------------------------------------------------------------------------
# 1. Schema check – ensure *exact* match with `EXPECTED_COLUMNS`.
# ---------------------------------------------------------------------------
@asset_check(asset=extract_pdf_data)
def check_expected_columns(df: pd.DataFrame) -> AssetCheckResult:  # noqa: D103
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

# ---------------------------------------------------------------------------
# 2. Duplicate‐key check – combination of dept/table/key must be unique.
# ---------------------------------------------------------------------------
@asset_check(asset=extract_pdf_data)
def check_no_duplicate_keys(df: pd.DataFrame) -> AssetCheckResult:  # noqa: D103
    dupes = df[df.duplicated(["department", "table name", "key"], keep=False)]
    passed = dupes.empty

    return AssetCheckResult(
        passed=passed,
        metadata={
            "duplicate_count": len(dupes),
            "example_duplicates": dupes["key"].unique().tolist()[:10],
        },
    )

# ---------------------------------------------------------------------------
# 3. Numeric columns – verify every cell parses as a number or is NaN.
# ---------------------------------------------------------------------------
@asset_check(asset=extract_pdf_data)
def check_numeric_columns_parse(df: pd.DataFrame) -> AssetCheckResult:  # noqa: D103
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

# ---------------------------------------------------------------------------
# 4. Row-level sanity checks – plug trusted rows here for regression tests.
# ---------------------------------------------------------------------------
ROW_SANITY_CHECKS: list[dict] = [
    # {
    #     "key": "All stops",
    #     "department": "Atchison County Sheriff's Department",
    #     "table name": "Rates by Race",
    #     "total": 22,
    # } 
]


def _make_row_sanity_check(asset, check: dict, idx: int, id_field: str = "key") -> AssetCheckSpec:  # noqa: D401,E501
    """Return an `asset_check` enforcing that *one* row matches `check`."""

    check_name = f"sanity_check_{idx}_{check.get(id_field, 'unknown')}"

    @asset_check(name=check_name, asset=asset)
    def _check(df: pd.DataFrame) -> AssetCheckResult:  # noqa: D401
        row = df[df[id_field] == check[id_field]]
        if row.empty:
            return AssetCheckResult(
                passed=False,
                metadata={"reason": f"{id_field} not found", id_field: check[id_field]},
            )

        mismatches = {
            key: (row.iloc[0].get(key), val)
            for key, val in check.items()
            if key != id_field and row.iloc[0].get(key) != val
        }

        return AssetCheckResult(
            passed=not mismatches,
            metadata={
                "checked_fields": list(check.keys()),
                "mismatches": mismatches,
            },
        )

    return _check


# Dynamically materialise any row-level checks defined above.
row_sanity_checks = [
    _make_row_sanity_check(extract_pdf_data, check, i)
    for i, check in enumerate(ROW_SANITY_CHECKS)
]

# Aggregate if you need to expose the list for Dagster’s loader utilities
asset_checks = [
    check_expected_columns,
    check_no_duplicate_keys,
    check_numeric_columns_parse,
    *row_sanity_checks,
]
