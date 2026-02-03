from __future__ import annotations

import json
from pathlib import Path
from typing import List

import pandas as pd
from slugify import slugify

from dagster import AssetIn, AssetKey, Out, graph_asset, op

from missouri_vsr.assets.extract import PIVOT_VALUE_COLUMNS, _slugify_simple
from missouri_vsr.assets.s3_utils import (
    upload_paths,
    s3_uri_for_dir,
    s3_uri_for_path,
    upload_file_to_s3,
)

# ------------------------------------------------------------------------------
# Pivoted outputs & per-agency JSON exports
# ------------------------------------------------------------------------------
MSHP_DEPARTMENT_NAME = "Missouri State Highway Patrol"
METRIC_YEAR_SUBSET_KEYS = [
    "rates-by-race--totals--all-stops",
    "rates-by-race--totals--arrests",
    "rates-by-race--totals--citations",
    "rates-by-race--totals--searches",
    "rates-by-race--totals--contraband",
    "rates-by-race--totals--resident-stops",
    "rates-by-race--population--acs-pop",
]


def _build_rank_percentile_frame(
    base: pd.DataFrame,
    metric_df: pd.DataFrame,
    value_cols: List[str],
    suffix: str,
) -> pd.DataFrame:
    frame = base[["agency", "year", "row_key"]].copy()
    frame["row_key"] = frame["row_key"].astype(str) + suffix
    for col in value_cols:
        frame[col] = metric_df[col]
    for col in base.columns:
        if col not in frame.columns:
            frame[col] = pd.NA
    return frame[base.columns]


def _build_percentage_frame(
    base: pd.DataFrame,
    value_cols: List[str],
    *,
    metric_col: str,
) -> pd.DataFrame:
    if metric_col not in base.columns:
        return pd.DataFrame(columns=base.columns)

    metric_series = base[metric_col].astype(str)
    non_rate_mask = ~metric_series.str.contains(r"\brate\b", case=False, regex=True, na=False)
    percentage_base = base.loc[non_rate_mask].copy()
    if percentage_base.empty:
        return pd.DataFrame(columns=base.columns)

    total = pd.to_numeric(percentage_base["Total"], errors="coerce")
    percentages = percentage_base[value_cols].div(total, axis=0)
    percentages = percentages.where(total > 0)
    percentage_base[value_cols] = percentages
    percentage_base["row_key"] = percentage_base["row_key"].astype(str) + "-percentage"
    return percentage_base


def _rebuild_row_ids(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"year", "agency", "row_key"}
    if not required.issubset(frame.columns):
        return frame

    def _format_year(value) -> str | None:
        if pd.isna(value):
            return None
        try:
            return str(int(value))
        except (TypeError, ValueError):
            return str(value)

    def _row_id(row: pd.Series) -> str:
        parts = [
            _format_year(row["year"]),
            _slugify_simple(str(row["agency"])),
            str(row["row_key"]),
        ]
        return "-".join(part for part in parts if part)

    updated = frame.copy()
    updated["row_id"] = updated.apply(_row_id, axis=1)
    return updated


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

    grouped = working.groupby(["year", "row_key"])[value_cols]
    rank_df = grouped.rank(method="dense", ascending=False)
    percentile_df = grouped.rank(method="max", ascending=True, pct=True) * 100

    if exclude_mask is None:
        return rank_df, percentile_df

    rank_full = pd.DataFrame(index=base.index, columns=value_cols, dtype="float")
    percentile_full = pd.DataFrame(index=base.index, columns=value_cols, dtype="float")
    rank_full.loc[working.index, :] = rank_df
    percentile_full.loc[working.index, :] = percentile_df
    return rank_full, percentile_full


@op(out=Out(pd.DataFrame), required_resource_keys={"data_dir_processed", "s3"})
def add_rank_percentile_rows(context, combined: pd.DataFrame) -> pd.DataFrame:
    """Add rank/percentile rows per row_key/year across agencies."""
    if combined.empty:
        context.log.warning("Combined report DataFrame is empty; skipping rank/percentile.")
        return combined

    required_cols = {"agency", "year", "row_key"}
    missing = required_cols - set(combined.columns)
    if missing:
        raise ValueError(f"Cannot add ranks/percentiles – missing columns: {sorted(missing)}")

    value_cols = [c for c in PIVOT_VALUE_COLUMNS if c in combined.columns]
    if not value_cols:
        raise ValueError("Cannot add ranks/percentiles – no numeric value columns were found.")

    base = combined.copy()
    metric_col = "metric"
    percentage_rows = _build_percentage_frame(base, value_cols, metric_col=metric_col)
    rank_all, percentile_all = _rank_and_percentile(base, value_cols)

    derived = [
        percentage_rows,
        _build_rank_percentile_frame(base, rank_all, value_cols, "-rank"),
        _build_rank_percentile_frame(base, percentile_all, value_cols, "-percentile"),
    ]
    augmented = pd.concat([base, *derived], ignore_index=True)
    augmented = _rebuild_row_ids(augmented)
    context.log.info(
        "Added rank/percentile/percentage rows: %d base rows → %d total rows",
        len(base),
        len(augmented),
    )

    out_dir = Path(context.resources.data_dir_processed.get_path())
    out_path = out_dir / "reports_with_rank_percentile.parquet"
    augmented.to_parquet(out_path, index=False, engine="pyarrow")
    context.log.info("Wrote rank/percentile Parquet → %s (%d rows)", out_path, len(augmented))

    meta = {"local_path": str(out_path), "row_count": len(augmented)}
    try:
        s3_meta = upload_file_to_s3(
            context,
            out_path,
            "downloads/reports_with_rank_percentile.parquet",
            content_type="application/vnd.apache.parquet",
        )
        if s3_meta:
            meta.update(s3_meta)
    except Exception as exc:
        context.log.exception("S3 handling encountered an exception for rank/percentile Parquet: %s", exc)

    try:
        context.add_output_metadata(meta)
    except Exception:
        pass
    return augmented


@graph_asset(
    name="reports_with_rank_percentile",
    group_name="processed",
    ins={"combine_all_reports": AssetIn(key=AssetKey("combine_all_reports"))},
    description="Add rank/percentile rows per row_key/year across agencies.",
)
def reports_with_rank_percentile(combine_all_reports: pd.DataFrame) -> pd.DataFrame:
    return add_rank_percentile_rows(combine_all_reports)


@op(out=Out(pd.DataFrame), required_resource_keys={"data_dir_processed", "s3"})
def compute_statewide_slug_baselines(context, combined: pd.DataFrame) -> pd.DataFrame:
    """Compute statewide mean/median baselines per year/row_key/metric."""
    baseline_columns = [
        "year",
        "row_key",
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

    required_cols = {"agency", "year", "row_key"}
    missing = required_cols - set(combined.columns)
    if missing:
        raise ValueError(f"Cannot compute baselines – missing columns: {sorted(missing)}")

    value_cols = [c for c in PIVOT_VALUE_COLUMNS if c in combined.columns]
    if not value_cols:
        raise ValueError("Cannot compute baselines – no numeric value columns were found.")

    base = combined[["agency", "year", "row_key", *value_cols]].copy()
    base["agency"] = base["agency"].astype(str).str.strip()

    def _build_baseline(df: pd.DataFrame, scope: str) -> pd.DataFrame:
        melted = df.melt(
            id_vars=["year", "row_key"],
            value_vars=value_cols,
            var_name="metric",
            value_name="value",
        )
        melted = melted.dropna(subset=["value"])
        if melted.empty:
            return pd.DataFrame(columns=["year", "row_key", "metric", "count", "mean", "median"])
        grouped = (
            melted.groupby(["year", "row_key", "metric"])["value"]
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
    no_mshp_baselines = _build_baseline(base.loc[base["agency"].ne(MSHP_DEPARTMENT_NAME)], "no_mshp")
    if all_baselines.empty and no_mshp_baselines.empty:
        baselines = pd.DataFrame(columns=baseline_columns)
    else:
        baselines = all_baselines.merge(
            no_mshp_baselines,
            on=["year", "row_key", "metric"],
            how="outer",
        )
        baselines = baselines[baseline_columns]

    out_dir = Path(context.resources.data_dir_processed.get_path())
    out_path = out_dir / "statewide_slug_baselines.parquet"
    baselines.to_parquet(out_path, index=False, engine="pyarrow")
    context.log.info("Wrote statewide baselines Parquet → %s (%d rows)", out_path, len(baselines))

    meta = {"local_path": str(out_path), "row_count": len(baselines)}
    try:
        s3_meta = upload_file_to_s3(
            context,
            out_path,
            "downloads/statewide_slug_baselines.parquet",
            content_type="application/vnd.apache.parquet",
        )
        if s3_meta:
            meta.update(s3_meta)
    except Exception as exc:
        context.log.exception("S3 handling encountered an exception for statewide baselines Parquet: %s", exc)

    try:
        context.add_output_metadata(meta)
    except Exception:
        pass
    return baselines


@graph_asset(
    name="statewide_slug_baselines",
    group_name="processed",
    ins={"combine_all_reports": AssetIn(key=AssetKey("combine_all_reports"))},
    description="Compute statewide mean/median baselines per year/row_key/metric.",
)
def statewide_slug_baselines(combine_all_reports: pd.DataFrame) -> pd.DataFrame:
    return compute_statewide_slug_baselines(combine_all_reports)


@op(out=Out(pd.DataFrame))
def pivot_reports_by_slug_op(context, combined: pd.DataFrame) -> pd.DataFrame:
    """Pivot combined report data so each Agency+Year row contains row_key-based columns."""
    if combined.empty:
        context.log.warning("Combined report DataFrame is empty; returning empty pivot result.")
        return pd.DataFrame(columns=["agency", "year"])

    required_cols = {"agency", "year", "row_key"}
    missing = required_cols - set(combined.columns)
    if missing:
        raise ValueError(f"Cannot pivot – missing columns: {sorted(missing)}")

    value_cols = [c for c in PIVOT_VALUE_COLUMNS if c in combined.columns]
    if not value_cols:
        raise ValueError("Cannot pivot – no numeric value columns were found.")

    base = combined[["agency", "year", "row_key", *value_cols]].copy()
    dup_mask = base.duplicated(subset=["agency", "year", "row_key"], keep=False)
    if dup_mask.any():
        dupe_preview = (
            base.loc[dup_mask, ["agency", "year", "row_key"]]
            .drop_duplicates()
            .head(5)
            .to_dict(orient="records")
        )
        context.log.warning(
            "Found %d duplicate row_key rows after grouping; keeping first occurrence. Examples: %s",
            dup_mask.sum(),
            dupe_preview,
        )

    pivoted = base.pivot_table(
        index=["agency", "year"],
        columns="row_key",
        values=value_cols,
        aggfunc="first",
    )

    if pivoted.empty:
        return pivoted.reset_index()

    if isinstance(pivoted.columns, pd.MultiIndex):
        pivoted = pivoted.sort_index(axis=1, level=0, sort_remaining=True)
        pivoted.columns = pivoted.columns.swaplevel(0, 1)
        pivoted.columns = [
            f"{row_key}__{metric}"
            for row_key, metric in pivoted.columns
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
    description="Pivot combined report data so each agency/year row has row_key-derived columns.",
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


@op(out=Out(list), required_resource_keys={"data_dir_out", "data_dir_processed", "s3"})
def write_agency_year_json(
    context,
    combined: pd.DataFrame,
    agency_reference_geocoded: pd.DataFrame,
) -> List[str]:
    """Write one JSON file per agency with row-level row_key metadata and values."""
    del agency_reference_geocoded
    if combined.empty:
        context.log.warning("Combined DataFrame empty; no JSON outputs created.")
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

    required_cols = {
        "agency",
        "year",
        "row_key",
        "table",
        "table_id",
        "section",
        "section_id",
        "metric",
        "metric_id",
        "row_id",
    }
    missing = required_cols - set(combined.columns)
    if missing:
        raise ValueError(f"Cannot write agency JSON – missing columns: {sorted(missing)}")

    value_cols = [c for c in PIVOT_VALUE_COLUMNS if c in combined.columns]
    row_cols = [
        "year",
        "row_key",
        "table",
        "table_id",
        "section",
        "section_id",
        "metric",
        "metric_id",
        "row_id",
        *value_cols,
    ]

    output_paths: List[str] = []
    for agency, group in combined.groupby("agency"):
        records: List[dict] = []
        for _, row in group.sort_values(["year", "row_key"]).iterrows():
            payload = {col: row.get(col) for col in row_cols if col in row.index}
            year_val = payload.get("year")
            payload["year"] = int(year_val) if pd.notna(year_val) else None
            records.append(_json_safe_record(payload))

        agency_slug = _agency_slug(agency)
        out_path = out_root / f"{agency_slug}.json"
        payload = {"agency": agency, "rows": records}
        meta_row = agency_meta_lookup.get(str(agency).strip())
        if meta_row is not None:
            payload["agency_metadata"] = _series_to_json_dict(meta_row)
        out_path.write_text(json.dumps(payload, indent=2))
        output_paths.append(str(out_path))
        context.log.info("Wrote %d year rows for %s → %s", len(records), agency, out_path)

    base_dir = Path(context.resources.data_dir_out.get_path())
    uploaded = upload_paths(
        context,
        [Path(path) for path in output_paths],
        base_dir=base_dir,
    )
    if uploaded:
        context.log.info("Uploaded %d agency-year JSON files to S3", len(uploaded))

    try:
        metadata = {"output_count": len(output_paths)}
        s3_folder = s3_uri_for_dir(context, out_root, base_dir)
        if s3_folder:
            metadata["s3_folder"] = s3_folder
        if uploaded:
            metadata["s3_paths"] = uploaded
        context.add_output_metadata(metadata)
    except Exception:
        pass

    return output_paths


@op(out=Out(list), required_resource_keys={"data_dir_out", "s3"})
def write_metric_year_json(context, combined: pd.DataFrame) -> List[str]:
    """Write one JSON file per row_key with agency/year and race columns."""
    if combined.empty:
        context.log.warning("Combined DataFrame empty; no metric-year JSON outputs created.")
        return []

    required_cols = {"agency", "year", "row_key"}
    missing = required_cols - set(combined.columns)
    if missing:
        raise ValueError(f"Cannot write metric-year JSON – missing columns: {sorted(missing)}")

    value_cols = [c for c in PIVOT_VALUE_COLUMNS if c in combined.columns]
    if not value_cols:
        raise ValueError("Cannot write metric-year JSON – no numeric value columns were found.")

    out_root = Path(context.resources.data_dir_out.get_path()) / "metric_year"
    out_root.mkdir(parents=True, exist_ok=True)

    output_paths: List[str] = []
    row_key_count = 0
    row_count = 0

    for row_key, group in combined.groupby("row_key"):
        if pd.isna(row_key) or str(row_key).strip() == "":
            continue
        row_key_count += 1
        records: List[dict] = []
        sorted_group = group.sort_values(["agency", "year"], na_position="last")
        for _, row in sorted_group.iterrows():
            agency = row.get("agency")
            year_val = row.get("year")
            year = int(year_val) if pd.notna(year_val) else None
            payload = {
                "agency": _json_safe_value(agency),
                "year": year,
            }
            for col in value_cols:
                payload[col] = _json_safe_value(row.get(col))
            records.append(payload)
        row_count += len(records)
        payload = {"row_key": str(row_key), "rows": records}
        out_path = out_root / f"{row_key}.json"
        out_path.write_text(json.dumps(payload, indent=2))
        output_paths.append(str(out_path))
        context.log.info("Wrote metric-year JSON → %s (%d rows)", out_path, len(records))

    base_dir = Path(context.resources.data_dir_out.get_path())
    uploaded = upload_paths(
        context,
        [Path(path) for path in output_paths],
        base_dir=base_dir,
    )
    if uploaded:
        context.log.info("Uploaded %d metric-year JSON files to S3", len(uploaded))

    try:
        metadata = {
            "output_count": len(output_paths),
            "row_key_count": row_key_count,
            "row_count": row_count,
        }
        s3_folder = s3_uri_for_dir(context, out_root, base_dir)
        if s3_folder:
            metadata["s3_folder"] = s3_folder
        if uploaded:
            metadata["s3_paths"] = uploaded
        context.add_output_metadata(metadata)
    except Exception:
        pass

    return output_paths


@op(out=Out(str), required_resource_keys={"data_dir_out", "s3"})
def write_metric_year_subset_json(context, combined: pd.DataFrame) -> str:
    """Write a compact JSON file with selected row_keys across agencies/years."""
    if combined.empty:
        context.log.warning("Combined DataFrame empty; no metric-year subset JSON created.")
        return ""

    required_cols = {"agency", "year", "row_key"}
    missing = required_cols - set(combined.columns)
    if missing:
        raise ValueError(f"Cannot write metric-year subset JSON – missing columns: {sorted(missing)}")

    value_cols = [c for c in PIVOT_VALUE_COLUMNS if c in combined.columns]
    if not value_cols:
        raise ValueError("Cannot write metric-year subset JSON – no numeric value columns were found.")

    out_root = Path(context.resources.data_dir_out.get_path())
    out_root.mkdir(parents=True, exist_ok=True)
    out_path = out_root / "metric_year_subset.json"

    subset = combined[combined["row_key"].isin(METRIC_YEAR_SUBSET_KEYS)].copy()
    subset = subset.sort_values(["row_key", "agency", "year"], na_position="last")

    agencies = (
        subset["agency"]
        .dropna()
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .unique()
        .tolist()
    )
    years = (
        pd.to_numeric(subset["year"], errors="coerce")
        .dropna()
        .astype(int)
        .unique()
        .tolist()
    )
    agencies = sorted(agencies)
    years = sorted(years)
    agency_index = {name: idx for idx, name in enumerate(agencies)}
    year_index = {year: idx for idx, year in enumerate(years)}

    rows_by_key: dict[str, list[list]] = {}
    total_rows = 0
    for key in METRIC_YEAR_SUBSET_KEYS:
        group = subset[subset["row_key"] == key]
        rows: List[list] = []
        for _, row in group.iterrows():
            agency = row.get("agency")
            agency_key = str(agency).strip() if pd.notna(agency) else None
            if not agency_key or agency_key not in agency_index:
                continue
            year_val = row.get("year")
            if pd.isna(year_val):
                continue
            try:
                year_val = int(year_val)
            except (TypeError, ValueError):
                continue
            if year_val not in year_index:
                continue
            entry = [agency_index[agency_key], year_index[year_val]]
            for col in value_cols:
                entry.append(_json_safe_value(row.get(col)))
            rows.append(entry)
        total_rows += len(rows)
        rows_by_key[key] = rows

    payload = {
        "agencies": agencies,
        "years": years,
        "columns": ["agency_idx", "year_idx", *value_cols],
        "rows": rows_by_key,
    }
    out_path.write_text(json.dumps(payload, separators=(",", ":")))
    context.log.info("Wrote metric-year subset JSON → %s (%d rows)", out_path, total_rows)

    base_dir = Path(context.resources.data_dir_out.get_path())
    uploaded = upload_paths(
        context,
        [out_path],
        base_dir=base_dir,
    )
    if uploaded:
        context.log.info("Uploaded metric-year subset JSON to S3")

    try:
        metadata = {
            "local_path": str(out_path),
            "row_key_count": len(METRIC_YEAR_SUBSET_KEYS),
            "row_count": total_rows,
        }
        s3_path = s3_uri_for_path(context, out_path, base_dir)
        if s3_path:
            metadata["s3_path"] = s3_path
        context.add_output_metadata(metadata)
    except Exception:
        pass

    return str(out_path)


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


@op(out=Out(str), required_resource_keys={"data_dir_out", "s3"})
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
    county_cols = ["geocode_jurisdiction_county", "geocode_address_county", "County"]
    metric_row_key = "rates-by-race--totals--all-stops"
    metric_col = f"{metric_row_key}__Total"
    metric_alias = "all_stops_total"

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
                    "county": None,
                    metric_row_key: None,
                    metric_alias: None,
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
            if not entry["county"]:
                entry["county"] = _first_non_empty(row, county_cols)

    if not pivoted.empty and "agency" in pivoted.columns:
        for agency in (
            pivoted["agency"].dropna().astype(str).str.strip().replace("", pd.NA).dropna().unique()
        ):
            entry = names_by_key.get(agency)
            if entry is None:
                entry = {
                    "agency_slug": _agency_slug(agency),
                    "canonical_name": agency,
                    "names": [agency],
                    "city": None,
                    "zip": None,
                    "phone": None,
                    "county": None,
                    metric_row_key: None,
                    metric_alias: None,
                }
                names_by_key[agency] = entry
            else:
                if agency not in entry["names"]:
                    entry["names"].append(agency)

    if not pivoted.empty and "agency" in pivoted.columns and metric_col in pivoted.columns:
        subset = pivoted[["agency", "year", metric_col]].copy()
        subset = subset[pd.notna(subset["agency"])]
        subset["agency"] = subset["agency"].astype(str).str.strip()
        subset = subset[subset["agency"].ne("")]
        subset["year"] = pd.to_numeric(subset["year"], errors="coerce")
        subset = subset.sort_values(["agency", "year"], ascending=[True, False])
        for agency, group in subset.groupby("agency", sort=False):
            value = None
            for item in group[metric_col].tolist():
                if _is_null(item):
                    continue
                value = _json_safe_value(item)
                break
            if value is None:
                continue
            entry = names_by_key.get(agency)
            if entry is None:
                entry = {
                    "agency_slug": _agency_slug(agency),
                    "canonical_name": agency,
                    "names": [agency],
                    "city": None,
                    "zip": None,
                    "phone": None,
                    "county": None,
                    metric_row_key: None,
                    metric_alias: None,
                }
                names_by_key[agency] = entry
            entry[metric_row_key] = value
            entry[metric_alias] = value

    records = sorted(names_by_key.values(), key=lambda item: item["agency_slug"])
    out_path.write_text(json.dumps(records, indent=2))
    context.log.info("Wrote agency index JSON → %s (%d rows)", out_path, len(records))
    base_dir = Path(context.resources.data_dir_out.get_path())
    uploaded = upload_paths(
        context,
        [out_path],
        base_dir=base_dir,
    )
    if uploaded:
        context.log.info("Uploaded agency index JSON to S3")
    try:
        metadata = {"local_path": str(out_path), "row_count": len(records)}
        s3_path = s3_uri_for_path(context, out_path, base_dir)
        if s3_path:
            metadata["s3_path"] = s3_path
        context.add_output_metadata(metadata)
    except Exception:
        pass
    return str(out_path)


@op(out=Out(str), required_resource_keys={"data_dir_out", "s3"})
def write_statewide_baselines_json(context, baselines: pd.DataFrame) -> str:
    """Write statewide baselines to JSON for downstream consumers."""
    out_root = Path(context.resources.data_dir_out.get_path())
    out_root.mkdir(parents=True, exist_ok=True)
    out_path = out_root / "statewide_slug_baselines.json"

    records = [_json_safe_record(item) for item in baselines.to_dict(orient="records")]
    out_path.write_text(json.dumps(records, indent=2))
    context.log.info("Wrote statewide baselines JSON → %s (%d rows)", out_path, len(records))
    base_dir = Path(context.resources.data_dir_out.get_path())
    uploaded = upload_paths(
        context,
        [out_path],
        base_dir=base_dir,
    )
    if uploaded:
        context.log.info("Uploaded statewide baselines JSON to S3")
    try:
        metadata = {"local_path": str(out_path), "row_count": len(records)}
        s3_path = s3_uri_for_path(context, out_path, base_dir)
        if s3_path:
            metadata["s3_path"] = s3_path
        context.add_output_metadata(metadata)
    except Exception:
        pass
    return str(out_path)


@op(out=Out(str), required_resource_keys={"data_dir_out", "s3"})
def write_statewide_year_sums_json(context, combined: pd.DataFrame) -> str:
    """Write statewide per-year sums for each row_key and race column."""
    if combined.empty:
        context.log.warning("Combined DataFrame empty; no statewide sums JSON created.")
        return ""

    required_cols = {"year", "row_key"}
    missing = required_cols - set(combined.columns)
    if missing:
        raise ValueError(f"Cannot write statewide sums JSON – missing columns: {sorted(missing)}")

    value_cols = [c for c in PIVOT_VALUE_COLUMNS if c in combined.columns]
    if not value_cols:
        raise ValueError("Cannot write statewide sums JSON – no numeric value columns were found.")

    subset = combined[["year", "row_key", *value_cols]].copy()
    if "row_key" in subset.columns:
        rate_mask = subset["row_key"].astype(str).str.endswith("-rate", na=False)
        if rate_mask.any():
            subset = subset.loc[~rate_mask].copy()
        pop_mask = subset["row_key"].astype(str).str.startswith(
            "rates-by-race--population", na=False
        )
        if pop_mask.any():
            subset = subset.loc[~pop_mask].copy()
    for col in value_cols:
        subset[col] = pd.to_numeric(subset[col], errors="coerce")

    grouped = (
        subset.groupby(["year", "row_key"], dropna=True)[value_cols]
        .sum(min_count=1)
        .reset_index()
        .sort_values(["year", "row_key"], na_position="last")
    )

    records = [_json_safe_record(item) for item in grouped.to_dict(orient="records")]

    out_root = Path(context.resources.data_dir_out.get_path())
    out_root.mkdir(parents=True, exist_ok=True)
    out_path = out_root / "statewide_year_sums.json"
    out_path.write_text(json.dumps(records, indent=2))
    context.log.info("Wrote statewide sums JSON → %s (%d rows)", out_path, len(records))

    base_dir = Path(context.resources.data_dir_out.get_path())
    uploaded = upload_paths(context, [out_path], base_dir=base_dir)
    if uploaded:
        context.log.info("Uploaded statewide sums JSON to S3")

    try:
        metadata = {
            "local_path": str(out_path),
            "row_count": len(records),
            "row_key_count": int(grouped["row_key"].nunique(dropna=True)),
            "year_count": int(grouped["year"].nunique(dropna=True)),
        }
        s3_path = s3_uri_for_path(context, out_path, base_dir)
        if s3_path:
            metadata["s3_path"] = s3_path
        context.add_output_metadata(metadata)
    except Exception:
        pass

    return str(out_path)


@op(out=Out(str), required_resource_keys={"data_dir_out", "s3"})
def write_report_dimension_index_json(context, combined: pd.DataFrame) -> str:
    """Write unique table/section/metric identifiers to JSON for translations."""
    out_root = Path(context.resources.data_dir_out.get_path())
    out_root.mkdir(parents=True, exist_ok=True)
    out_path = out_root / "report_dimensions.json"

    if combined.empty:
        payload = {"table_ids": [], "section_ids": [], "metric_ids": []}
    else:
        table_ids = (
            combined["table_id"]
            .dropna()
            .astype(str)
            .str.strip()
            .replace("", pd.NA)
            .dropna()
            .unique()
            .tolist()
        )
        section_ids = (
            combined["section_id"]
            .dropna()
            .astype(str)
            .str.strip()
            .replace("", pd.NA)
            .dropna()
            .unique()
            .tolist()
        )
        metric_ids = (
            combined["metric_id"]
            .dropna()
            .astype(str)
            .str.strip()
            .replace("", pd.NA)
            .dropna()
            .unique()
            .tolist()
        )
        payload = {
            "table_ids": sorted(table_ids),
            "section_ids": sorted(section_ids),
            "metric_ids": sorted(metric_ids),
        }

    out_path.write_text(json.dumps(payload, indent=2))
    context.log.info(
        "Wrote report dimension index JSON → %s (tables=%d sections=%d metrics=%d)",
        out_path,
        len(payload["table_ids"]),
        len(payload["section_ids"]),
        len(payload["metric_ids"]),
    )
    base_dir = Path(context.resources.data_dir_out.get_path())
    uploaded = upload_paths(
        context,
        [out_path],
        base_dir=base_dir,
    )
    if uploaded:
        context.log.info("Uploaded report dimension index JSON to S3")
    try:
        metadata = {
            "local_path": str(out_path),
            "table_id_count": len(payload["table_ids"]),
            "section_id_count": len(payload["section_ids"]),
            "metric_id_count": len(payload["metric_ids"]),
        }
        s3_path = s3_uri_for_path(context, out_path, base_dir)
        if s3_path:
            metadata["s3_path"] = s3_path
        context.add_output_metadata(metadata)
    except Exception:
        pass
    return str(out_path)


@graph_asset(
    name="agency_year_json_exports",
    group_name="output",
    ins={
        "reports_with_rank_percentile": AssetIn(key=AssetKey("reports_with_rank_percentile")),
        "agency_reference_geocoded": AssetIn(key=AssetKey("agency_reference_geocoded")),
    },
    description="Generate per-agency JSON files containing year-by-year row_key data.",
)
def agency_year_json_exports(
    reports_with_rank_percentile: pd.DataFrame,
    agency_reference_geocoded: pd.DataFrame,
) -> List[str]:
    return write_agency_year_json(reports_with_rank_percentile, agency_reference_geocoded)


@graph_asset(
    name="metric_year_json_exports",
    group_name="output",
    ins={"combine_all_reports": AssetIn(key=AssetKey("combine_all_reports"))},
    description="Generate per-row_key JSON files with agency/year/race values.",
)
def metric_year_json_exports(combine_all_reports: pd.DataFrame) -> List[str]:
    return write_metric_year_json(combine_all_reports)


@graph_asset(
    name="metric_year_subset_json",
    group_name="output",
    ins={"combine_all_reports": AssetIn(key=AssetKey("combine_all_reports"))},
    description="Write a compact JSON file with selected row_keys across agencies/years.",
)
def metric_year_subset_json(combine_all_reports: pd.DataFrame) -> str:
    return write_metric_year_subset_json(combine_all_reports)


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


@graph_asset(
    name="statewide_year_sums_json",
    group_name="output",
    ins={"combine_all_reports": AssetIn(key=AssetKey("combine_all_reports"))},
    description="Write statewide per-year sums for each row_key and race column.",
)
def statewide_year_sums_json(combine_all_reports: pd.DataFrame) -> str:
    return write_statewide_year_sums_json(combine_all_reports)


@graph_asset(
    name="report_dimension_index_json",
    group_name="output",
    ins={"combine_all_reports": AssetIn(key=AssetKey("combine_all_reports"))},
    description="Write unique table/section/metric identifiers for translations.",
)
def report_dimension_index_json(combine_all_reports: pd.DataFrame) -> str:
    return write_report_dimension_index_json(combine_all_reports)
