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
    "section"
]

NUMERIC_COLS = EXPECTED_COLUMNS[1:8]  # Just the race-specific numbers

# Row-level sanity checks – plug trusted rows here for regression tests.
ROW_SANITY_CHECKS: list[dict] = [
    # The first one
    {
        "slug": "rates--totals--all-stops",
        "department": "Adair County Sheriff's Dept",
        "Total": 317,
        "White": 281,
        "Black": 25,
        "Hispanic": 8,
        "Native American": 0,
        "Asian": 2,
        "Other": 1,
    }
    # A random one
    # {
    #     "slug": "stops--all-stops--resident-stops",
    #     "department": "Lake Winnebago Police Dept",
    #     "Total": 109,
    #     "White": 105,
    #     "Black": 1,
    #     "Hispanic": 0,
    #     "Native American": 0,
    #     "Asian": 0,
    #     "Other": 3,
    # },
    # Now a big important dept
    {
        "slug": "rates--totals--all-stops",
        "department": "St Louis City Police Dept",
        "Total": 27742,
        "White": 10077,
        "Black": 16424,
        "Hispanic": 551,
        "Native American": 50,
        "Asian": 241,
        "Other": 399,
    },
    # Now values that can be decimal numbers (again with St. Louis City)
    {
        "slug": "rates--rates--stop-rate",
        "department": "St Louis City Police Dept",
        "Total": 11.18,
        "White": 8.44,
        "Black": 16.01,
        "Hispanic": 5.92,
        "Native American": 9.75,
        "Asian": 2.56,
        "Other": 3.15,
    },
    # A dept with an apostrophe in the name AND missing values
    # {
    #     "slug": "rates--rates--search-rate",
    #     "department": "Benton County Sheriff's Dept",
    #     "Total": 20.47,
    #     "White": 20.08,
    #     "Black": 25,
    #     "Hispanic": 33.33,
    #     "Native American": None,
    #     "Asian": None,
    #     "Other": None,
    # },
    # That same department has zeros for some rates as well
    # {
    #     "slug": "rates--rates--stop-rate",
    #     "department": "Benton County Sheriff's Dept",
    #     "Total": 3.09,
    #     "White": 3.17,
    #     "Black": 11.85,
    #     "Hispanic": 2.73,
    #     "Native American": 0,
    #     "Asian": 0,
    #     "Other": 0,
    # },
]

# Schema check for extracted pdf data – ensure *exact* match with `EXPECTED_COLUMNS`.
@asset_check(asset=extract_pdf_data)
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

# Duplicate‐slug check – combination of dept/slug must be unique.
@asset_check(asset=extract_pdf_data)
def check_no_duplicate_slugs(df: pd.DataFrame) -> AssetCheckResult:
    dupes = df[df.duplicated(["department", "slug"], keep=False)]
    passed = dupes.empty

    # Include both department and slug for each duplicate row
    example_duplicates = dupes[["department", "slug"]].drop_duplicates().to_dict(orient="records")

    return AssetCheckResult(
        passed=passed,
        metadata={
            "duplicate_count": len(dupes),
            "example_duplicates": example_duplicates,
        },
    )

# Verify every numeric cell parses as a number or is NaN.
@asset_check(asset=extract_pdf_data)
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
