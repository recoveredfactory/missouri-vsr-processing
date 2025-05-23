import pandas as pd

from slugify import slugify
from dagster import AssetCheckResult, AssetCheckSpec, asset_check
from missouri_vsr.assets import extract_pdf_data

EXPECTED_COLUMNS = [
    "key",
    "Total",
    "White",
    "Black",
    "Hispanic",
    "Native American",
    "Asian",
    "Other",
    "slug",
    "department",
    "table name",
]

NUMERIC_COLS = EXPECTED_COLUMNS[1:8]  # Just the race-specific numbers

# Schema check for extracted pdf data – ensure *exact* match with `EXPECTED_COLUMNS`.
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

# Duplicate‐slug check – combination of dept/table/slug must be unique.
@asset_check(asset=extract_pdf_data)
def check_no_duplicate_slugs(df: pd.DataFrame) -> AssetCheckResult:
    dupes = df[df.duplicated(["department", "table name", "slug"], keep=False)]
    passed = dupes.empty

    return AssetCheckResult(
        passed=passed,
        metadata={
            "duplicate_count": len(dupes),
            "example_duplicates": dupes["key"].unique().tolist()[:10],
        },
    )

# Verify every numeric cell parses as a number or is NaN.
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

# Row-level sanity checks – plug trusted rows here for regression tests.
ROW_SANITY_CHECKS: list[dict] = [
    {
        "slug": "stops-resident-stops",
        "department": "Andrew County Sheriff's Dept",
        "White": 634,
    } 
]

def _make_row_sanity_check(asset, check: dict, idx: int) -> AssetCheckSpec:
    """Return an `asset_check` enforcing that *one* row matches `check`."""

    check_slug = check["slug"].replace("-", "_")
    dept_slug = slugify(check["department"], separator="_")
    check_name = f"sanity_check_{idx}_{check_slug}_{dept_slug}"

    @asset_check(name=check_name, asset=asset)
    def _check(df: pd.DataFrame) -> AssetCheckResult:
        # Ensure both conditions are applied correctly with parentheses
        row = df[(df["slug"] == check["slug"]) & (df["department"] == check["department"])]
        if row.empty:
            return AssetCheckResult(
                passed=False,
                metadata={"reason": f"{check['slug']} + {check['department']} not found"},
            )

        # Check all additional fields in the dict (besides slug/department)
        mismatches = {}
        for key, val in check.items():
            if key in ("slug", "department"):
                continue
            actual_val = row.iloc[0][key]
            if actual_val != val:
                mismatches[key] = {"expected": val, "actual": actual_val}

        return AssetCheckResult(
            passed=not mismatches,
            metadata={
                "checked_fields": [k for k in check.keys() if k not in ("slug", "department")],
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
    check_no_duplicate_slugs,
    check_numeric_columns_parse,
    *row_sanity_checks,
]
