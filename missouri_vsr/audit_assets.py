from __future__ import annotations

from pathlib import Path

import pandas as pd

from dagster import AssetIn, AssetKey, Out, graph_asset, op

from missouri_vsr.assets import NUMERIC_COLS


@op(out=Out(pd.DataFrame), required_resource_keys={"data_dir_processed"})
def audit_race_sum_mismatch_total_op(context, combined: pd.DataFrame) -> pd.DataFrame:
    """Flag rows where race subtotals do not match Total for rates--totals--all-stops."""
    out_dir = Path(context.resources.data_dir_processed.get_path()) / "audits"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "race_sum_mismatch_total.parquet"

    required_cols = {"Department", "year", "slug", "Total"}
    missing = required_cols - set(combined.columns)
    if missing:
        raise ValueError(f"Cannot audit race totals – missing columns: {sorted(missing)}")

    slug_target = "rates--totals--all-stops"
    races = [c for c in NUMERIC_COLS if c != "Total" and c in combined.columns]
    if not races:
        raise ValueError("Cannot audit race totals – no race columns were found.")

    empty_frame = pd.DataFrame(
        columns=[
            "Department",
            "year",
            "slug",
            "Total",
            "race_sum",
            "diff",
            "pct_diff",
            "White",
            "Black",
            "Hispanic",
            "Native American",
            "Asian",
            "Other",
        ]
    )

    if combined.empty:
        context.log.warning("Combined report DataFrame is empty; no audit rows produced.")
        audit_rows = empty_frame
    else:
        subset = combined[combined["slug"] == slug_target].copy()
        if subset.empty:
            context.log.warning("No rows found for slug %s; audit output empty.", slug_target)
            audit_rows = subset
        else:
            needed = ["Total", *races]
            subset = subset.dropna(subset=needed)
            if subset.empty:
                context.log.warning("No rows with complete race totals for slug %s.", slug_target)
                audit_rows = subset
            else:
                subset["race_sum"] = subset[races].sum(axis=1)
                subset["diff"] = subset["race_sum"] - subset["Total"]
                total_safe = subset["Total"].replace({0: pd.NA})
                subset["pct_diff"] = subset["diff"] / total_safe
                audit_rows = subset[subset["diff"] != 0].copy()

    out_cols = [
        "Department",
        "year",
        "slug",
        "Total",
        "race_sum",
        "diff",
        "pct_diff",
        *races,
    ]
    audit_rows = audit_rows[out_cols] if not audit_rows.empty else audit_rows.reindex(columns=out_cols)

    audit_rows.to_parquet(out_path, index=False, engine="pyarrow")
    context.log.info("Wrote race-total audit Parquet → %s (%d rows)", out_path, len(audit_rows))
    try:
        context.add_output_metadata(
            {
                "local_path": str(out_path),
                "output_file": out_path.name,
                "row_count": len(audit_rows),
            }
        )
    except Exception:
        pass
    return audit_rows


@graph_asset(
    name="audit_race_sum_mismatch_total",
    group_name="vsr_audit",
    ins={"combine_all_reports": AssetIn(key=AssetKey("combine_all_reports"))},
    description="Find rows where race subtotals do not match Total for rates--totals--all-stops.",
)
def audit_race_sum_mismatch_total_asset(combine_all_reports: pd.DataFrame) -> pd.DataFrame:
    return audit_race_sum_mismatch_total_op(combine_all_reports)
