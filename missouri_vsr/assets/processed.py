from __future__ import annotations

import json
from pathlib import Path
from typing import List

import pandas as pd
from slugify import slugify

from dagster import AssetIn, AssetKey, Out, graph_asset, op

from missouri_vsr.assets.extract import PIVOT_VALUE_COLUMNS

# ------------------------------------------------------------------------------
# Pivoted outputs & per-agency JSON exports
# ------------------------------------------------------------------------------
MSHP_DEPARTMENT_NAME = "Missouri State Highway Patrol"


def _build_rank_percentile_frame(
    base: pd.DataFrame,
    metric_df: pd.DataFrame,
    value_cols: List[str],
    suffix: str,
) -> pd.DataFrame:
    frame = base[["Department", "year", "slug"]].copy()
    frame["slug"] = frame["slug"].astype(str) + suffix
    for col in value_cols:
        frame[col] = metric_df[col]
    for col in base.columns:
        if col not in frame.columns:
            frame[col] = pd.NA
    return frame[base.columns]


def _rank_and_percentile(
    base: pd.DataFrame,
    value_cols: List[str],
    *,
    exclude_mask: pd.Series | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if exclude_mask is not None:
        working = base.loc[~exclude_mask]
    else:
        working = base

    if working.empty:
        blank = pd.DataFrame(index=base.index, columns=value_cols, dtype="float")
        return blank.copy(), blank.copy()

    grouped = working.groupby(["year", "slug"])[value_cols]
    rank_df = grouped.rank(method="dense", ascending=False)
    percentile_df = grouped.rank(method="max", ascending=True, pct=True) * 100

    if exclude_mask is None:
        return rank_df, percentile_df

    rank_full = pd.DataFrame(index=base.index, columns=value_cols, dtype="float")
    percentile_full = pd.DataFrame(index=base.index, columns=value_cols, dtype="float")
    rank_full.loc[working.index, :] = rank_df
    percentile_full.loc[working.index, :] = percentile_df
    return rank_full, percentile_full


@op(out=Out(pd.DataFrame))
def add_rank_percentile_rows(context, combined: pd.DataFrame) -> pd.DataFrame:
    """Add rank/percentile rows per slug/year across agencies."""
    if combined.empty:
        context.log.warning("Combined report DataFrame is empty; skipping rank/percentile.")
        return combined

    required_cols = {"Department", "year", "slug"}
    missing = required_cols - set(combined.columns)
    if missing:
        raise ValueError(f"Cannot add ranks/percentiles – missing columns: {sorted(missing)}")

    value_cols = [c for c in PIVOT_VALUE_COLUMNS if c in combined.columns]
    if not value_cols:
        raise ValueError("Cannot add ranks/percentiles – no numeric value columns were found.")

    base = combined.copy()
    rank_all, percentile_all = _rank_and_percentile(base, value_cols)

    derived = [
        _build_rank_percentile_frame(base, rank_all, value_cols, "-rank"),
        _build_rank_percentile_frame(base, percentile_all, value_cols, "-percentile"),
    ]
    augmented = pd.concat([base, *derived], ignore_index=True)
    context.log.info(
        "Added rank/percentile rows: %d base rows → %d total rows",
        len(base),
        len(augmented),
    )
    return augmented


@graph_asset(
    name="reports_with_rank_percentile",
    group_name="processed",
    ins={"combine_all_reports": AssetIn(key=AssetKey("combine_all_reports"))},
    description="Add rank/percentile rows per slug/year across agencies.",
)
def reports_with_rank_percentile(combine_all_reports: pd.DataFrame) -> pd.DataFrame:
    return add_rank_percentile_rows(combine_all_reports)


@op(out=Out(pd.DataFrame), required_resource_keys={"data_dir_processed"})
def compute_statewide_slug_baselines(context, combined: pd.DataFrame) -> pd.DataFrame:
    """Compute statewide mean/median baselines per year/slug/metric."""
    baseline_columns = [
        "year",
        "slug",
        "metric",
        "count",
        "mean",
        "median",
        "count__no_mshp",
        "mean__no_mshp",
        "median__no_mshp",
    ]
    if combined.empty:
        context.log.warning("Combined report DataFrame is empty; no baselines computed.")
        return pd.DataFrame(columns=baseline_columns)

    required_cols = {"Department", "year", "slug"}
    missing = required_cols - set(combined.columns)
    if missing:
        raise ValueError(f"Cannot compute baselines – missing columns: {sorted(missing)}")

    value_cols = [c for c in PIVOT_VALUE_COLUMNS if c in combined.columns]
    if not value_cols:
        raise ValueError("Cannot compute baselines – no numeric value columns were found.")

    base = combined[["Department", "year", "slug", *value_cols]].copy()
    base["Department"] = base["Department"].astype(str).str.strip()

    def _build_baseline(df: pd.DataFrame, scope: str) -> pd.DataFrame:
        melted = df.melt(
            id_vars=["year", "slug"],
            value_vars=value_cols,
            var_name="metric",
            value_name="value",
        )
        melted = melted.dropna(subset=["value"])
        if melted.empty:
            return pd.DataFrame(columns=["year", "slug", "metric", "count", "mean", "median"])
        grouped = (
            melted.groupby(["year", "slug", "metric"])["value"]
            .agg(count="count", mean="mean", median="median")
            .reset_index()
        )
        if scope == "no_mshp":
            grouped = grouped.rename(
                columns={
                    "count": "count__no_mshp",
                    "mean": "mean__no_mshp",
                    "median": "median__no_mshp",
                }
            )
        return grouped

    all_baselines = _build_baseline(base, "all")
    no_mshp_baselines = _build_baseline(base.loc[base["Department"].ne(MSHP_DEPARTMENT_NAME)], "no_mshp")
    if all_baselines.empty and no_mshp_baselines.empty:
        baselines = pd.DataFrame(columns=baseline_columns)
    else:
        baselines = all_baselines.merge(
            no_mshp_baselines,
            on=["year", "slug", "metric"],
            how="outer",
        )
        baselines = baselines[baseline_columns]

    out_dir = Path(context.resources.data_dir_processed.get_path())
    out_path = out_dir / "statewide_slug_baselines.parquet"
    baselines.to_parquet(out_path, index=False, engine="pyarrow")
    context.log.info("Wrote statewide baselines Parquet → %s (%d rows)", out_path, len(baselines))
    try:
        context.add_output_metadata({"local_path": str(out_path), "row_count": len(baselines)})
    except Exception:
        pass
    return baselines


@graph_asset(
    name="statewide_slug_baselines",
    group_name="processed",
    ins={"combine_all_reports": AssetIn(key=AssetKey("combine_all_reports"))},
    description="Compute statewide mean/median baselines per year/slug/metric.",
)
def statewide_slug_baselines(combine_all_reports: pd.DataFrame) -> pd.DataFrame:
    return compute_statewide_slug_baselines(combine_all_reports)


@op(out=Out(pd.DataFrame))
def pivot_reports_by_slug_op(context, combined: pd.DataFrame) -> pd.DataFrame:
    """Pivot combined report data so each Agency+Year row contains slug-based columns."""
    if combined.empty:
        context.log.warning("Combined report DataFrame is empty; returning empty pivot result.")
        return pd.DataFrame(columns=["Department", "year"])

    required_cols = {"Department", "year", "slug"}
    missing = required_cols - set(combined.columns)
    if missing:
        raise ValueError(f"Cannot pivot – missing columns: {sorted(missing)}")

    value_cols = [c for c in PIVOT_VALUE_COLUMNS if c in combined.columns]
    if not value_cols:
        raise ValueError("Cannot pivot – no numeric value columns were found.")

    base = combined[["Department", "year", "slug", *value_cols]].copy()
    dup_mask = base.duplicated(subset=["Department", "year", "slug"], keep=False)
    if dup_mask.any():
        dupe_preview = (
            base.loc[dup_mask, ["Department", "year", "slug"]]
            .drop_duplicates()
            .head(5)
            .to_dict(orient="records")
        )
        context.log.warning(
            "Found %d duplicate slug rows after grouping; keeping first occurrence. Examples: %s",
            dup_mask.sum(),
            dupe_preview,
        )

    pivoted = base.pivot_table(
        index=["Department", "year"],
        columns="slug",
        values=value_cols,
        aggfunc="first",
    )

    if pivoted.empty:
        return pivoted.reset_index()

    if isinstance(pivoted.columns, pd.MultiIndex):
        pivoted = pivoted.sort_index(axis=1, level=0, sort_remaining=True)
        pivoted.columns = pivoted.columns.swaplevel(0, 1)
        pivoted.columns = [
            f"{slug}__{metric}"
            for slug, metric in pivoted.columns
        ]
    else:
        pivoted.columns = [str(c) for c in pivoted.columns]

    pivoted = pivoted.reset_index()
    pivoted["year"] = pivoted["year"].astype("Int64", copy=False)
    return pivoted


@graph_asset(
    name="pivot_reports_by_slug",
    group_name="processed",
    ins={"reports_with_rank_percentile": AssetIn(key=AssetKey("reports_with_rank_percentile"))},
    description="Pivot combined report data so each agency/year row has slug-derived columns.",
)
def pivot_reports_by_slug(reports_with_rank_percentile: pd.DataFrame) -> pd.DataFrame:
    return pivot_reports_by_slug_op(reports_with_rank_percentile)


def _is_null(value) -> bool:
    try:
        result = pd.isna(value)
    except Exception:
        return False
    if isinstance(result, bool):
        return result
    try:
        return bool(result)
    except Exception:
        return False


def _json_safe_value(value):
    if _is_null(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return item()
        except Exception:
            pass
    return value


def _series_to_json_dict(series: pd.Series) -> dict:
    data = {}
    for col, value in series.items():
        if _is_null(value):
            continue
        if col.endswith("_response") and isinstance(value, str):
            try:
                data[col] = json.loads(value)
                continue
            except ValueError:
                pass
        data[col] = _json_safe_value(value)
    return data


def _agency_slug(value) -> str:
    if value is None:
        return "agency"
    text = str(value).replace("'", "").strip()
    if not text or text.lower() == "nan":
        return "agency"
    slug = slugify(text, lowercase=True)
    return slug or "agency"


def _json_safe_record(record: dict) -> dict:
    cleaned = {}
    for key, value in record.items():
        if _is_null(value):
            cleaned[key] = None
        else:
            cleaned[key] = _json_safe_value(value)
    return cleaned


@op(out=Out(list), required_resource_keys={"data_dir_out", "data_dir_processed"})
def write_agency_year_json(
    context,
    pivoted: pd.DataFrame,
    agency_reference_geocoded: pd.DataFrame,
) -> List[str]:
    """Write one JSON file per agency containing rows for each year."""
    del agency_reference_geocoded
    if pivoted.empty:
        context.log.warning("Pivoted DataFrame empty; no JSON outputs created.")
        return []

    out_root = Path(context.resources.data_dir_out.get_path()) / "agency_year"
    out_root.mkdir(parents=True, exist_ok=True)

    agency_meta_lookup: dict[str, pd.Series] = {}
    processed_dir = Path(context.resources.data_dir_processed.get_path())
    reference_path = processed_dir / "agency_reference_geocoded.parquet"
    if not reference_path.exists():
        reference_path = processed_dir / "agency_reference.parquet"

    if reference_path.exists():
        try:
            reference_df = pd.read_parquet(reference_path)
            join_col = next(
                (c for c in ["Canonical", "Department", "Agency", "Name"] if c in reference_df.columns),
                None,
            )
            if not join_col:
                context.log.warning(
                    "agency_reference.parquet missing expected join column (Canonical/Department/Agency/Name); skipping metadata"
                )
            else:
                reference_df = reference_df.copy()
                reference_df = reference_df[pd.notna(reference_df[join_col])]
                reference_df[join_col] = reference_df[join_col].astype(str).str.strip()
                reference_df = reference_df[reference_df[join_col].ne("")]
                dup_mask = reference_df.duplicated(subset=[join_col], keep=False)
                if dup_mask.any():
                    dupe_sample = (
                        reference_df.loc[dup_mask, join_col]
                        .drop_duplicates()
                        .head(5)
                        .tolist()
                    )
                    context.log.warning(
                        "agency_reference has duplicate %s values; keeping first. Sample: %s",
                        join_col,
                        dupe_sample,
                    )
                reference_df = reference_df.drop_duplicates(subset=[join_col], keep="first")
                agency_meta_lookup = {
                    str(key): row
                    for key, row in reference_df.set_index(join_col).iterrows()
                }
                context.log.info(
                    "Loaded agency reference metadata for %d agencies from %s",
                    len(agency_meta_lookup),
                    reference_path,
                )
        except Exception as e:
            context.log.exception("Failed to load agency reference metadata: %s", e)
    else:
        context.log.info("Agency reference metadata not found at %s; skipping metadata", reference_path)

    output_paths: List[str] = []
    for department, group in pivoted.groupby("Department"):
        records: List[dict] = []
        for _, row in group.sort_values("year").iterrows():
            row_payload: dict = {}
            year_val = row.get("year")
            if pd.notna(year_val):
                row_payload["year"] = int(year_val)
            else:
                row_payload["year"] = None

            for col, value in row.items():
                if col in {"Department", "year"}:
                    continue
                if pd.isna(value):
                    continue
                if "__" not in col:
                    row_payload[col] = value
                    continue
                slug, metric = col.split("__", 1)
                slug_bucket = row_payload.setdefault(slug, {})
                slug_bucket[metric] = value
            records.append(row_payload)

        agency_slug = _agency_slug(department)
        out_path = out_root / f"{agency_slug}.json"
        payload = {"agency": department, "rows": records}
        meta_row = agency_meta_lookup.get(str(department).strip())
        if meta_row is not None:
            payload["agency_metadata"] = _series_to_json_dict(meta_row)
        out_path.write_text(json.dumps(payload, indent=2))
        output_paths.append(str(out_path))
        context.log.info("Wrote %d year rows for %s → %s", len(records), department, out_path)

    return output_paths


def _collect_names(row: pd.Series, name_cols: list[str]) -> list[str]:
    seen = set()
    names: list[str] = []
    for col in name_cols:
        if col not in row:
            continue
        value = row.get(col)
        if _is_null(value):
            continue
        text = str(value).strip()
        if not text or text.lower() == "nan":
            continue
        if text not in seen:
            seen.add(text)
            names.append(text)
    return names


def _first_non_empty(row: pd.Series, columns: list[str]) -> str | None:
    for col in columns:
        if col not in row:
            continue
        value = row.get(col)
        if _is_null(value):
            continue
        text = str(value).strip()
        if text and text.lower() != "nan":
            return text
    return None


@op(out=Out(str), required_resource_keys={"data_dir_out"})
def write_agency_index_json(
    context,
    pivoted: pd.DataFrame,
    agency_reference_geocoded: pd.DataFrame,
) -> str:
    """Write a JSON index of agencies for search/discovery."""
    out_root = Path(context.resources.data_dir_out.get_path())
    out_root.mkdir(parents=True, exist_ok=True)
    out_path = out_root / "agency_index.json"

    names_by_key: dict[str, dict] = {}
    name_cols = [
        "Department",
        "Canonical",
        "AgencyName",
        "Agency Name",
        "Agency",
        "DepartmentName",
        "Name",
        "Raw",
    ]
    city_cols = ["AddressCity", "City"]
    zip_cols = ["AddressZip", "Zip", "ZipCode"]
    phone_cols = ["Phone", "PhoneNumber", "Phone #"]
    metric_slug = "rates--totals--all-stops"
    metric_col = f"{metric_slug}__Total"

    if not agency_reference_geocoded.empty:
        for _, row in agency_reference_geocoded.iterrows():
            display_name = _first_non_empty(row, name_cols)
            if not display_name:
                continue
            canonical = _first_non_empty(row, ["Department", "Canonical"])
            key = canonical or display_name
            entry = names_by_key.get(key)
            if entry is None:
                entry = {
                    "agency_slug": _agency_slug(canonical or display_name),
                    "canonical_name": canonical,
                    "names": [],
                    "city": None,
                    "zip": None,
                    "phone": None,
                    metric_slug: None,
                }
                names_by_key[key] = entry

            new_names = _collect_names(row, name_cols)
            for name in new_names:
                if name not in entry["names"]:
                    entry["names"].append(name)
            if not entry["city"]:
                entry["city"] = _first_non_empty(row, city_cols)
            if not entry["zip"]:
                entry["zip"] = _first_non_empty(row, zip_cols)
            if not entry["phone"]:
                entry["phone"] = _first_non_empty(row, phone_cols)

    if not pivoted.empty and "Department" in pivoted.columns:
        for dept in (
            pivoted["Department"].dropna().astype(str).str.strip().replace("", pd.NA).dropna().unique()
        ):
            entry = names_by_key.get(dept)
            if entry is None:
                entry = {
                    "agency_slug": _agency_slug(dept),
                    "canonical_name": dept,
                    "names": [dept],
                    "city": None,
                    "zip": None,
                    "phone": None,
                    metric_slug: None,
                }
                names_by_key[dept] = entry
            else:
                if dept not in entry["names"]:
                    entry["names"].append(dept)

    if not pivoted.empty and "Department" in pivoted.columns and metric_col in pivoted.columns:
        subset = pivoted[["Department", "year", metric_col]].copy()
        subset = subset[pd.notna(subset["Department"])]
        subset["Department"] = subset["Department"].astype(str).str.strip()
        subset = subset[subset["Department"].ne("")]
        subset["year"] = pd.to_numeric(subset["year"], errors="coerce")
        subset = subset.sort_values(["Department", "year"], ascending=[True, False])
        for dept, group in subset.groupby("Department", sort=False):
            value = None
            for item in group[metric_col].tolist():
                if _is_null(item):
                    continue
                value = _json_safe_value(item)
                break
            if value is None:
                continue
            entry = names_by_key.get(dept)
            if entry is None:
                entry = {
                    "agency_slug": _agency_slug(dept),
                    "canonical_name": dept,
                    "names": [dept],
                    "city": None,
                    "zip": None,
                    "phone": None,
                    metric_slug: None,
                }
                names_by_key[dept] = entry
            entry[metric_slug] = value

    records = sorted(names_by_key.values(), key=lambda item: item["agency_slug"])
    out_path.write_text(json.dumps(records, indent=2))
    context.log.info("Wrote agency index JSON → %s (%d rows)", out_path, len(records))
    try:
        context.add_output_metadata({"local_path": str(out_path), "row_count": len(records)})
    except Exception:
        pass
    return str(out_path)


@op(out=Out(str), required_resource_keys={"data_dir_out"})
def write_statewide_baselines_json(context, baselines: pd.DataFrame) -> str:
    """Write statewide baselines to JSON for downstream consumers."""
    out_root = Path(context.resources.data_dir_out.get_path())
    out_root.mkdir(parents=True, exist_ok=True)
    out_path = out_root / "statewide_slug_baselines.json"

    records = [_json_safe_record(item) for item in baselines.to_dict(orient="records")]
    out_path.write_text(json.dumps(records, indent=2))
    context.log.info("Wrote statewide baselines JSON → %s (%d rows)", out_path, len(records))
    try:
        context.add_output_metadata({"local_path": str(out_path), "row_count": len(records)})
    except Exception:
        pass
    return str(out_path)


@graph_asset(
    name="agency_year_json_exports",
    group_name="output",
    ins={
        "pivot_reports_by_slug": AssetIn(key=AssetKey("pivot_reports_by_slug")),
        "agency_reference_geocoded": AssetIn(key=AssetKey("agency_reference_geocoded")),
    },
    description="Generate per-agency JSON files containing year-by-year slug data.",
)
def agency_year_json_exports(
    pivot_reports_by_slug: pd.DataFrame,
    agency_reference_geocoded: pd.DataFrame,
) -> List[str]:
    return write_agency_year_json(pivot_reports_by_slug, agency_reference_geocoded)


@graph_asset(
    name="agency_index_json",
    group_name="output",
    ins={
        "pivot_reports_by_slug": AssetIn(key=AssetKey("pivot_reports_by_slug")),
        "agency_reference_geocoded": AssetIn(key=AssetKey("agency_reference_geocoded")),
    },
    description="Generate an agency index JSON for search and lookup.",
)
def agency_index_json(
    pivot_reports_by_slug: pd.DataFrame,
    agency_reference_geocoded: pd.DataFrame,
) -> str:
    return write_agency_index_json(pivot_reports_by_slug, agency_reference_geocoded)


@graph_asset(
    name="statewide_slug_baselines_json",
    group_name="output",
    ins={"statewide_slug_baselines": AssetIn(key=AssetKey("statewide_slug_baselines"))},
    description="Generate statewide baselines JSON for downstream consumers.",
)
def statewide_slug_baselines_json(statewide_slug_baselines: pd.DataFrame) -> str:
    return write_statewide_baselines_json(statewide_slug_baselines)
