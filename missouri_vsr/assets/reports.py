from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from dagster import AssetIn, AssetKey, In, Out, graph_asset, op

from missouri_vsr.assets.extract import YEAR_URLS
from missouri_vsr.assets.s3_utils import upload_file_to_s3

# ------------------------------------------------------------------------------
# Combine all extracted DataFrame assets into one JSON and DataFrame
# ------------------------------------------------------------------------------
def _normalize_acs_population_rows(combined: pd.DataFrame, log) -> pd.DataFrame:
    if "row_key" not in combined.columns:
        return combined

    row_key_series = combined["row_key"].astype(str)
    pattern = r"^.+--population--\d{4}-(?:acs-)?pop(?:-pct)?$"
    mask = row_key_series.str.match(pattern, na=False)
    if not mask.any():
        return combined

    old_keys = row_key_series[mask]
    new_keys = old_keys.str.replace(r"--\d{4}-(?:acs-)?pop", "--acs-pop", regex=True)
    combined.loc[mask, "row_key"] = new_keys.values

    if "metric_id" in combined.columns:
        combined.loc[mask, "metric_id"] = (
            combined.loc[mask, "metric_id"]
            .astype(str)
            .str.replace(r"^\d{4}-(?:acs-)?pop", "acs-pop", regex=True)
        )

    if "metric" in combined.columns:
        combined.loc[mask, "metric"] = (
            combined.loc[mask, "metric"]
            .astype(str)
            .str.replace(r"^\d{4}\s+(?:ACS\s+)?pop\s*%", "ACS pop %", regex=True)
            .str.replace(r"^\d{4}\s+(?:ACS\s+)?pop", "ACS pop", regex=True)
        )

    if "row_id" in combined.columns:
        row_ids = combined.loc[mask, "row_id"].astype(str)
        combined.loc[mask, "row_id"] = [
            row_id.replace(old_key, new_key) if old_key in row_id else row_id
            for row_id, old_key, new_key in zip(row_ids, old_keys, new_keys)
        ]

    log.info("Normalized ACS population row_key values (%d rows).", int(mask.sum()))
    return combined


@op(
    ins={f"extract_pdf_data_{year}": In(pd.DataFrame) for year in YEAR_URLS},
    out=Out(pd.DataFrame),
    required_resource_keys={"data_dir_processed", "s3"},
)
def combine_reports(context, **extracted_reports: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Concatenate all extract_pdf_data_* assets into a single DataFrame and write combined Parquet."""
    # Merge all DataFrames
    dfs = [df for df in extracted_reports.values() if not df.empty]
    if not dfs:
        raise ValueError("No extracted tables found to combine.")
    combined = pd.concat(dfs, ignore_index=True)
    legacy_cols = ["Key", "Department", "Table name", "Measurement"]
    drop_cols = [col for col in legacy_cols if col in combined.columns]
    if drop_cols:
        context.log.info("Dropping legacy columns from combined output: %s", drop_cols)
        combined = combined.drop(columns=drop_cols)
    if "row_key" not in combined.columns and "slug" in combined.columns:
        context.log.info("Renaming legacy slug column to row_key in combined output.")
        combined = combined.rename(columns={"slug": "row_key"})
    if "slug" in combined.columns:
        combined = combined.drop(columns=["slug"])

    combined = _normalize_acs_population_rows(combined, context.log)

    # Write combined Parquet
    processed_dir = Path(context.resources.data_dir_processed.get_path())
    out_file = processed_dir / "all_combined_output.parquet"
    combined.to_parquet(out_file, index=False, engine="pyarrow")
    context.log.info("Wrote combined Parquet: %d rows → %s", len(combined), out_file)

    # Attach local path and optional S3 metadata
    meta = {"local_path": str(out_file)}
    try:
        s3_meta = upload_file_to_s3(
            context,
            out_file,
            "downloads/combined/all_combined_output.parquet",
            content_type="application/vnd.apache.parquet",
        )
        if s3_meta:
            meta.update(s3_meta)
            if "s3_uri" in s3_meta:
                context.log.info("Combined Parquet available at %s", s3_meta["s3_uri"])
    except Exception as e:
        context.log.exception("S3 handling encountered an exception for combined Parquet: %s", e)

    # Add the same row-count/uniques metadata as per-year extracts.
    try:
        meta["row_count"] = int(len(combined))
        if "agency" in combined.columns:
            meta["unique_agencies"] = int(combined["agency"].nunique(dropna=True))
        if "row_key" in combined.columns:
            meta["unique_row_keys"] = int(combined["row_key"].nunique(dropna=True))
        context.add_output_metadata(meta)
        context.log.info("combine_all_reports metadata: %s", meta)
    except Exception:
        pass

    return combined


@graph_asset(
    name="combine_all_reports",
    group_name="processed",
    ins={
        f"extract_pdf_data_{year}": AssetIn(key=AssetKey(f"extract_pdf_data_{year}"))
        for year in YEAR_URLS
    },
    description="Combine all per-year extract_pdf_data_* assets into a single JSON and DataFrame."
)
def combine_all_reports(**extracted_reports: pd.DataFrame) -> pd.DataFrame:
    return combine_reports(**extracted_reports)
