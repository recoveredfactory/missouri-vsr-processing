from __future__ import annotations

import json
import os
import re
import unicodedata
from pathlib import Path
from typing import Dict, List

import camelot
import pandas as pd
import requests
from PyPDF2 import PdfReader
from slugify import slugify

from dagster import (
    AssetIn,
    AssetOut,
    AssetKey,
    In,
    Out,
    DynamicOut,
    DynamicOutput,
    Output,
    graph_asset,
    multi_asset,
    op,
)

# ------------------------------------------------------------------------------
# Configure the years you want Dagster to ingest
# ------------------------------------------------------------------------------
YEAR_URLS: Dict[int, str] = {
    2024: "https://ago.mo.gov/wp-content/uploads/2024-VSR-Agency-Specific-Reports.pdf",
    2023: "https://ago.mo.gov/wp-content/uploads/VSRreport2023.pdf",
    2022: "https://ago.mo.gov/wp-content/uploads/vsrreport2022.pdf",
    2021: "https://ago.mo.gov/wp-content/uploads/2021-VSR-Agency-Specific-Report.pdf",
    2020: "https://ago.mo.gov/wp-content/uploads/2020-VSR-Agency-Specific-Report.pdf",
}

# ------------------------------------------------------------------------------
# Download PDFs (one logical asset per year)
# ------------------------------------------------------------------------------
@multi_asset(
    outs={str(year): AssetOut(metadata={"url": url}) for year, url in YEAR_URLS.items()},
    group_name="reports",
    can_subset=True,
    required_resource_keys={"data_dir_report_pdfs"},
    description="Download VSR PDFs for every configured year.",
)
def download_reports(context):
    # figure out which outputs we should actually produce
    selected = (
        set(context.selected_output_names)
        if context.selected_output_names
        else set(str(y) for y in YEAR_URLS)
    )

    out_dir = Path(context.resources.data_dir_report_pdfs.get_path())

    for year, url in YEAR_URLS.items():
        name = str(year)
        if name not in selected:
            context.log.debug("Skipping %s (not selected)", name)
            continue

        # If the file already exists locally, re-use it and skip the download
        # Otherwise, download to the reports directory.

        out_path = out_dir / f"VSRreport{year}.pdf"

        # only hit the network if we haven't already downloaded it
        if not out_path.exists():
            context.log.info("Downloading %s → %s", url, out_path)
            # (10 s to connect, 300 s to read)
            resp = requests.get(url, timeout=(10, 300))
            resp.raise_for_status()
            out_path.write_bytes(resp.content)
        else:
            context.log.debug("Using cached %s", out_path)

        yield Output(
            str(out_path),
            output_name=name,
            metadata={"mode": "reports", "url": url, "local_path": str(out_path)},
        )

# ------------------------------------------------------------------------------
# PDF parsing helpers
# ------------------------------------------------------------------------------

def _env_positive_int(name: str, default: int) -> int:
    """Parse a positive int from env var *name*, falling back to *default*."""
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        val = int(raw)
        return val if val > 0 else default
    except ValueError:
        return default


def _env_csv(name: str, default: List[str]) -> List[str]:
    """Parse a comma-separated env var into a list."""
    raw = os.getenv(name)
    if not raw:
        return default
    items = [item.strip() for item in raw.split(",") if item.strip()]
    return items or default


PAGE_CHUNK_SIZE = _env_positive_int("PAGE_CHUNK_SIZE", 5)  # tune as needed

FINAL_COLUMNS = [
    "Key",
    "Total",
    "White",
    "Black",
    "Hispanic",
    "Native American",
    "Asian",
    "Other",
    "Department",
    "Table name",
    "Measurement",
]
NUMERIC_COLS = FINAL_COLUMNS[1:8]
PIVOT_VALUE_COLUMNS = NUMERIC_COLS

GEOCODIO_FIELDS = _env_csv(
    "GEOCODIO_FIELDS",
    [
        "cd",
        "stateleg",
        "stateleg-next",
        "census2024",
        "census2023",
        "census2022",
        "census2021",
        "census2020",
        "acs-demographics",
        "acs-economics",
        "acs-social",
        "ffiec",
    ],
)

TABLE_SLUG_LOOKUP = {
    "Rates by Race": "rates",
    "Number of Stops by Race": "stops",
    "Search statistics": "search",
}

def _normalize_text(text: str) -> str:
    """Strip ligatures & smart quotes to plain ASCII."""
    text = unicodedata.normalize("NFKC", text)
    return text.replace("ﬀ", "ff").replace("’", "'").replace(".", "")

# Accept numbers like 10, 10.5, 1,234.56, and leading-dot decimals like .27, with optional trailing %
_NUM_RE = re.compile(r"^(?:\d[\d,]*\.?\d*|\.\d+)%?$")
_DOTS_RE = re.compile(r"^[\.·]{2,}$")

def _is_numeric(tok: str) -> bool:
    tok = tok.strip()
    # Treat only real numbers (with optional commas/decimals) as numeric.
    # Dot leaders like "." are NOT considered numeric to avoid false positives.
    return bool(_NUM_RE.match(tok))

def _clean_numeric_str(tok: str) -> str:
    t = tok.strip().replace(",", "")
    return t[:-1] if t.endswith("%") else t

def normalize_row_tokens(row: list[str], dept_name: str, table_slug: str, log) -> list[str]:
    log.debug("Raw row tokens: %s / %s %s", list(row), table_slug, dept_name)
    row = [str(c).strip() for c in row if str(c).strip()]

    # Merge split decimals before numeric sniffing (e.g., '2 . 89' → '2.89', '. 27' → '0.27')
    merged: list[str] = []
    i = 0
    while i < len(row):
        tok = row[i]
        # Only merge when the right-hand numeric token has at least 2 digits to avoid fusing '0 . 0'
        if tok == '.' and i + 1 < len(row) and row[i + 1].isdigit() and len(row[i + 1]) >= 2:
            merged.append(f"0.{row[i + 1]}")
            i += 2
            continue
        if (
            i + 2 < len(row)
            and row[i + 1] == '.'
            and row[i].isdigit()
            and row[i + 2].isdigit()
            and len(row[i + 2]) >= 2
        ):
            merged.append(f"{row[i]}.{row[i + 2]}")
            i += 3
            continue
        merged.append(tok)
        i += 1
    # Remove dot-leader filler tokens (.., ..., ···) after merging
    row = [t for t in merged if not _DOTS_RE.match(t)]

    # Normalise tokens for numeric detection only (keep original tokens for key)
    def _norm_num_token(t: str) -> str:
        s = t.strip()
        # Fix common OCR-ish confusions
        if s.lower() == 'o':
            s = '0'
        # Strip trailing paired punctuation that can cling to numbers
        s = s.rstrip(')]')
        return s
    row_num = [_norm_num_token(t) for t in row]

    # Pick the rightmost 7 slots that are numeric OR a lone '.' placeholder
    # Treat a single '.' as an explicit missing slot to preserve column alignment.
    numeric_positions = [
        idx
        for idx, tok in enumerate(row_num)
        if _is_numeric(tok) or tok == "."
    ]
    picked_positions = numeric_positions[-7:]
    numeric = [row_num[idx] for idx in picked_positions]

    # Build key from non-picked tokens, preserving original order
    picked_set = set(picked_positions)
    key_tokens = [tok for idx, tok in enumerate(row) if idx not in picked_set]

    if len(numeric) < 7:
        log.warning("Found only %d numeric cols: %s", len(numeric), numeric)
        # Pad on the RIGHT so present values align to early columns (Total, White, …)
        numeric = numeric + [""] * (7 - len(numeric))
    elif len(numeric) > 7:
        log.debug("Extra numeric cols %s – keeping rightmost 7", numeric[:-7])
        numeric = numeric[-7:]

    key = " ".join(key_tokens).strip() or "(blank key)"

    # Convert any '.' placeholders into explicit blanks and leave numeric tokens as-is.
    # Light touch: do not scale integers heuristically (preserve source values for auditing).
    numeric = ["" if v == "." else v for v in numeric]

    return [key] + numeric


def _clean_camelot_table(table, log, *, year: int) -> pd.DataFrame | None:
    """Return a tidy DataFrame for one Camelot table (with ``year`` column)."""
    df = table.df.copy()
    log.debug("Raw Camelot table shape: %s", df.shape)
    df = df.dropna(axis=1, how="all")

    metadata_row_idx = metadata_line = None
    for i, row in df.iterrows():
        line = " ".join(row.dropna())
        if re.search(r"Table\s*\d+:", line):
            metadata_row_idx, metadata_line = i, line
            break
    if metadata_row_idx is None:
        log.warning("Missing metadata row – skipping table")
        return None

    m = re.search(r"Table\s*(\d+):\s*(.*?)\s*for\s*(.+)", metadata_line)
    if not m:
        log.warning("Could not parse metadata line: %s", metadata_line)
        return None

    raw_table_name, raw_dept = m.group(2).strip(), m.group(3).strip()
    dept_name = _normalize_text(raw_dept)
    table_slug = TABLE_SLUG_LOOKUP.get(raw_table_name, slugify(raw_table_name, lowercase=True))

    # Slice away the metadata rows ("Table N: …") and a possible blank spacer row
    df = df.iloc[metadata_row_idx + 2 :].reset_index(drop=True)
    if pd.isna(df.iloc[0, 0]) or str(df.iloc[0, 0]).strip() == "":
        df = df.iloc[:, 1:]

    # Detect indentation per original Camelot row using cell x-coordinates
    # Align Camelot cells to the sliced DataFrame rows
    start_row_idx = int(metadata_row_idx) + 2
    aligned_cell_rows = table.cells[start_row_idx : start_row_idx + len(df)]

    left_x: list[float] = []
    for r in aligned_cell_rows:
        xs = [float(c.x1) for c in r if str(c.text).strip()]
        left_x.append(min(xs) if xs else float("inf"))
    finite_lefts = [x for x in left_x if x != float("inf")]
    min_left = min(finite_lefts) if finite_lefts else float("inf")
    # Treat rows as indented if their left edge is meaningfully to the right
    indent_threshold = 1.5  # PDF coordinate units; small but effective
    is_indented_row = [(lx - min_left) > indent_threshold for lx in left_x]

    # Build normalized token rows from text-only df; keep a flag for numeric presence
    has_numeric_flags: List[bool] = []
    normalized_rows: List[List[str]] = []
    for _, row in df.iterrows():
        toks = [str(c).strip() for c in row if str(c).strip()]
        has_numeric = any(_is_numeric(t) for t in toks)
        has_numeric_flags.append(has_numeric)
        normalized_rows.append(
            normalize_row_tokens(list(toks), dept_name, table_slug, log)
        )
    df = pd.DataFrame(normalized_rows, columns=FINAL_COLUMNS[:8])

    df["Key"] = (
        df["Key"]
        .astype(str)
        .str.replace(r"\s*\n\s*", " ", regex=True)
        .str.strip()
        .str.lower()
        .apply(_normalize_text)
    )

    # Attach sections: rows without numbers AND without indentation are section headers, then ffill
    df["Measurement"] = None
    header_mask_list: list[bool] = [
        (not has_numeric_flags[i]) and (not is_indented_row[i]) for i in range(len(df))
    ]
    for i, is_header in enumerate(header_mask_list):
        if is_header:
            df.loc[i, "Measurement"] = df.loc[i, "Key"].strip()
    section_header_mask = pd.Series(header_mask_list, index=df.index)

    df["Measurement"] = df["Measurement"].ffill()

    # Remove blank rows, notes, and the section-header rows themselves
    mask_blank_key = df["Key"].str.strip().eq("") | df["Key"].isna()
    mask_notes = df["Key"].str.contains(r"^notes?\s*:\s*", case=False, na=False)
    df = df[~(mask_blank_key | mask_notes | section_header_mask)].copy()
    if df.empty:
        log.warning("All rows removed after cleanup – skipping table")
        return None

    # Convert numerics. Keep NaN for blanks.
    for col in NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col].map(_clean_numeric_str), errors="coerce")

    # Drop rows that contain no numeric data at all (filters leftovers)
    mask_all_na_numeric = df[NUMERIC_COLS].isna().all(axis=1)
    if mask_all_na_numeric.any():
        log.debug("Dropping %d rows with no numeric values", int(mask_all_na_numeric.sum()))
    df = df[~mask_all_na_numeric].copy()
    if df.empty:
        log.warning("Only non-numeric rows found – skipping table")
        return None

    def _build_slug(row: pd.Series) -> str:
        parts: list[str] = [table_slug]
        if row.Measurement:
            parts.extend(["", slugify(str(row.Measurement), lowercase=True, replacements=[["%", "pct"]]), ""])
        parts.append(slugify(str(row.Key), lowercase=True, replacements=[["%", "pct"]]))
        return "-".join(parts)

    df["slug"] = df.apply(_build_slug, axis=1)
    df["Department"] = dept_name
    df["Table name"] = raw_table_name
    df["year"] = year
    return df

# ------------------------------------------------------------------------------
# Page-range fan-out / fan-in ops
# ------------------------------------------------------------------------------
@op(out=DynamicOut(str))
def calculate_page_ranges(context, pdf_path: str):
    """Yield page-range strings (size = PAGE_CHUNK_SIZE)."""
    total_pages = len(PdfReader(pdf_path).pages)
    for i in range(1, total_pages + 1, PAGE_CHUNK_SIZE):
        page_range = f"{i}-{min(i + PAGE_CHUNK_SIZE - 1, total_pages)}"
        yield DynamicOutput(page_range, mapping_key=page_range.replace("-", "_"))

@op(out=Out(pd.DataFrame))
def parse_page_range(context, pdf_path: str, page_range: str) -> pd.DataFrame:
    """Extract every table on *page_range* from *pdf_path* and annotate with year."""
    try:
        tables = camelot.read_pdf(
            pdf_path,
            pages=page_range,
            flavor="stream",
            edge_tol=50,
            row_tol=0,
            strip_text="\n",
        )
    except Exception as exc:
        context.log.error("Camelot failed on %s: %s", page_range, exc)
        return pd.DataFrame()

    year_match = re.search(r"(\d{4})", Path(pdf_path).name)
    year = int(year_match.group(1)) if year_match else None

    # --- DEBUG DUMP ---
    # debug_dir = Path('debug') / f"{year}"
    # debug_dir.mkdir(parents=True, exist_ok=True)
    # for idx, table in enumerate(tables):
    #     for kind in ['grid', 'contour', 'text']:
    #         img_path = debug_dir / f"{page_range.replace('-', '_')}_tbl{idx}_{kind}.png"
    #         # Camelot can save directly via filename=…
    #         camelot.plot(table, kind=kind, filename=str(img_path))
    #         context.log.info("Wrote debug image %s", img_path)  # you’ll see this in the run log

    frames = [
        cleaned
        for t in tables
        if (cleaned := _clean_camelot_table(t, context.log, year=year)) is not None and not cleaned.empty
    ]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

@op(out=Out(pd.DataFrame), required_resource_keys={"data_dir_processed"})
def concat_and_write_parquet(context, chunks: List[pd.DataFrame], pdf_path: str) -> pd.DataFrame:
    """Merge page chunks, write Parquet, return combined DataFrame."""
    non_empty = [c for c in chunks if not c.empty]
    if not non_empty:
        raise ValueError("No tables were extracted from the PDF.")
    combined = pd.concat(non_empty, ignore_index=True)

    year_match = re.search(r"(\d{4})", Path(pdf_path).name)
    year = year_match.group(1) if year_match else "unknown"

    out_parquet = context.resources.data_dir_processed.get_path() / f"combined_output_{year}.parquet"
    combined.to_parquet(out_parquet, index=False, engine="pyarrow")
    context.log.info("Wrote %d rows (Parquet) → %s", len(combined), out_parquet)

    # Attach local path and optional S3 metadata to the asset materialization
    meta = {"local_path": str(out_parquet)}
    try:
        s3_res = getattr(context.resources, "s3", None)
        if s3_res is not None and hasattr(s3_res, "upload_file"):
            bucket = getattr(s3_res, "bucket", None)
            s3_prefix = getattr(s3_res, "s3_prefix", "") or ""
            key_prefix = (s3_prefix.strip("/") + "/" if s3_prefix else "") + "missouri-vsr/"
            key = f"{key_prefix}{year}/combined_output_{year}.parquet"
            presigned = s3_res.upload_file(str(out_parquet), key)
            uri = f"s3://{bucket}/{key}"
            meta.update({"s3_uri": uri})
            if presigned:
                meta.update({"presigned_url": presigned})
            context.log.info("Uploaded per-year Parquet to %s", uri)
    except Exception as e:
        context.log.warning("S3 upload skipped/failed: %s", e)

    try:
        context.add_output_metadata(meta)
    except Exception:
        pass

    return combined

# ------------------------------------------------------------------------------
# Extract graph assets factory
# ------------------------------------------------------------------------------
def make_extract_asset(year: int):
    @graph_asset(
        name=f"extract_pdf_data_{year}",
        group_name="extract",
        ins={"pdf_path": AssetIn(key=AssetKey(str(year)))},
        description=f"Extract tabular data from the {year} VSR (parallelised per page range).",
    )
    def extract_for_year(pdf_path: str) -> pd.DataFrame:
        page_ranges = calculate_page_ranges(pdf_path)
        processed = page_ranges.map(lambda pr: parse_page_range(pdf_path, pr))
        return concat_and_write_parquet(processed.collect(), pdf_path)

    extract_for_year.__name__ = f"extract_pdf_data_{year}"
    return extract_for_year

# Instantiate one extract asset per year
for yr in YEAR_URLS:
    globals()[f"extract_pdf_data_{yr}"] = make_extract_asset(yr)

# ------------------------------------------------------------------------------
# Combine all extracted DataFrame assets into one JSON and DataFrame
# ------------------------------------------------------------------------------
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

    # Write combined Parquet
    processed_dir = Path(context.resources.data_dir_processed.get_path())
    out_file = processed_dir / "all_combined_output.parquet"
    combined.to_parquet(out_file, index=False, engine="pyarrow")
    context.log.info("Wrote combined Parquet: %d rows → %s", len(combined), out_file)

    # Attach local path and optional S3 metadata
    meta = {"local_path": str(out_file)}
    try:
        s3_res = getattr(context.resources, "s3", None)
        if s3_res is not None:
            # Resolve bucket/prefix from resource config or env.
            resolver_bucket = getattr(s3_res, "resolved_bucket", None)
            bucket = resolver_bucket() if callable(resolver_bucket) else getattr(s3_res, "bucket", None)
            if not bucket:
                bucket = os.getenv("MISSOURI_BUCKET_NAME") or os.getenv("AWS_S3_BUCKET")
            s3_prefix = getattr(s3_res, "resolved_prefix", None)
            prefix_clean = s3_prefix() if callable(s3_prefix) else (getattr(s3_res, "s3_prefix", "") or "").strip("/")

            if not bucket:
                context.log.warning("S3 resource present but no bucket configured (env MISSOURI_BUCKET_NAME/AWS_S3_BUCKET). Skipping upload.")
                meta["s3_upload_error"] = "Missing bucket configuration."
                context.add_output_metadata(meta)
                return combined

            context.log.info("S3 configured; uploading combined Parquet… [bucket=%s, prefix=%s]", bucket, prefix_clean)
            key_prefix = f"{prefix_clean}/" if prefix_clean else ""
            key = f"{key_prefix}combined/all_combined_output.parquet"
            import boto3
            from botocore.config import Config
            cfg = Config(signature_version="s3v4")
            region = getattr(s3_res, "resolved_region", lambda: None)()
            if region:
                client = boto3.client("s3", region_name=region, config=cfg)
            else:
                client = boto3.client("s3", config=cfg)
            try:
                client.upload_file(str(out_file), bucket, key, ExtraArgs={"ContentType": "application/vnd.apache.parquet"})
            except Exception as e:
                context.log.exception("S3 upload failed for combined Parquet: %s", e)
                meta["s3_upload_error"] = str(e)
            uri = f"s3://{bucket}/{key}"
            meta["s3_uri"] = uri
            try:
                # Default to 45 days if not configured.
                expires = int(getattr(s3_res, "presigned_expiration", 45 * 24 * 60 * 60))
                presigned = client.generate_presigned_url(
                    ClientMethod="get_object",
                    Params={"Bucket": bucket, "Key": key},
                    ExpiresIn=expires,
                )
                meta["presigned_url"] = presigned
                context.log.info("Generated presigned URL for combined Parquet")
            except Exception as e:
                context.log.exception("Presign failed for combined Parquet: %s", e)
                meta["s3_presign_error"] = str(e)
            context.log.info("Combined Parquet available at %s", uri)
        else:
            context.log.debug("No S3 resource configured; skipping S3 upload for combined Parquet")
    except Exception as e:
        context.log.exception("S3 handling encountered an exception for combined Parquet: %s", e)

    try:
        context.add_output_metadata(meta)
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
    """Add rank/percentile rows per slug/year across agencies (with and without MSHP)."""
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
    dept_series = base["Department"].astype(str).str.strip()
    mshp_mask = dept_series.eq(MSHP_DEPARTMENT_NAME)

    rank_all, percentile_all = _rank_and_percentile(base, value_cols)
    rank_no_mshp, percentile_no_mshp = _rank_and_percentile(base, value_cols, exclude_mask=mshp_mask)

    derived = [
        _build_rank_percentile_frame(base, rank_all, value_cols, "-rank"),
        _build_rank_percentile_frame(base, percentile_all, value_cols, "-percentile"),
        _build_rank_percentile_frame(base, rank_no_mshp, value_cols, "-rank-no-mshp"),
        _build_rank_percentile_frame(base, percentile_no_mshp, value_cols, "-percentile-no-mshp"),
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
    if combined.empty:
        context.log.warning("Combined report DataFrame is empty; no baselines computed.")
        return pd.DataFrame(columns=["year", "slug", "metric", "scope", "count", "mean", "median"])

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
            return pd.DataFrame(columns=["year", "slug", "metric", "count", "mean", "median", "scope"])
        grouped = (
            melted.groupby(["year", "slug", "metric"])["value"]
            .agg(count="count", mean="mean", median="median")
            .reset_index()
        )
        grouped["scope"] = scope
        return grouped

    all_baselines = _build_baseline(base, "all")
    no_mshp_baselines = _build_baseline(base.loc[base["Department"].ne(MSHP_DEPARTMENT_NAME)], "no_mshp")
    baselines = pd.concat([all_baselines, no_mshp_baselines], ignore_index=True)

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
        data[col] = _json_safe_value(value)
    return data


@op(out=Out(list), required_resource_keys={"data_dir_out", "data_dir_processed"})
def write_agency_year_json(context, pivoted: pd.DataFrame) -> List[str]:
    """Write one JSON file per agency containing rows for each year."""
    if pivoted.empty:
        context.log.warning("Pivoted DataFrame empty; no JSON outputs created.")
        return []

    out_root = Path(context.resources.data_dir_out.get_path()) / "agency_year"
    out_root.mkdir(parents=True, exist_ok=True)

    agency_meta_lookup: dict[str, pd.Series] = {}
    reference_path = Path(context.resources.data_dir_processed.get_path()) / "agency_reference.parquet"
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
        context.log.info("agency_reference.parquet not found at %s; skipping metadata", reference_path)

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

        agency_slug = slugify(str(department), lowercase=True)
        if not agency_slug:
            agency_slug = "agency"
        out_path = out_root / f"{agency_slug}.json"
        payload = {"agency": department, "rows": records}
        meta_row = agency_meta_lookup.get(str(department).strip())
        if meta_row is not None:
            payload["agency_metadata"] = _series_to_json_dict(meta_row)
        out_path.write_text(json.dumps(payload, indent=2))
        output_paths.append(str(out_path))
        context.log.info("Wrote %d year rows for %s → %s", len(records), department, out_path)

    return output_paths


@graph_asset(
    name="agency_year_json_exports",
    group_name="processed",
    ins={"pivot_reports_by_slug": AssetIn(key=AssetKey("pivot_reports_by_slug"))},
    description="Generate per-agency JSON files containing year-by-year slug data.",
)
def agency_year_json_exports(pivot_reports_by_slug: pd.DataFrame) -> List[str]:
    return write_agency_year_json(pivot_reports_by_slug)

# ------------------------------------------------------------------------------
# Agency list (from Excel) → Parquet asset
# ------------------------------------------------------------------------------
@op(out=Out(pd.DataFrame), required_resource_keys={"data_dir_source", "data_dir_processed"})
def load_agency_list(context) -> pd.DataFrame:
    """Load the agencies Excel source into a DataFrame (no join yet)."""
    src_dir = Path(context.resources.data_dir_source.get_path())
    xlsx = src_dir / "2025-05-05-post-law-enforcement-agencies-list.xlsx"
    if not xlsx.exists():
        raise FileNotFoundError(f"Missing agencies Excel: {xlsx}")
    df = pd.read_excel(xlsx, engine="openpyxl")
    # Best-effort standardization: strip whitespace in potential name columns
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).str.strip()

    # Write a Parquet copy for downstream (e.g., DuckDB/Observable)
    out_dir = Path(context.resources.data_dir_processed.get_path())
    out_path = out_dir / "agency_list.parquet"
    df.to_parquet(out_path, index=False, engine="pyarrow")
    context.log.info("Wrote agency list Parquet → %s (%d rows)", out_path, len(df))
    try:
        context.add_output_metadata({"local_path": str(out_path)})
    except Exception:
        pass
    return df

@graph_asset(name="agency_list", group_name="agency_reference")
def agency_list_asset() -> pd.DataFrame:
    return load_agency_list()

# ------------------------------------------------------------------------------
# Agency list geocoding (Geocodio)
# ------------------------------------------------------------------------------
def _clean_geocodio_zip(value) -> str:
    if pd.isna(value):
        return ""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return f"{int(value):05d}"
    text = str(value).strip()
    if not text:
        return ""
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return ""
    return digits.zfill(5)[:5]


def _clean_geocodio_text(value) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return ""
    return text


def _build_geocodio_query(row: pd.Series) -> str:
    line1 = _clean_geocodio_text(row.get("AddressLine1", ""))
    line2 = _clean_geocodio_text(row.get("AddressLine2", ""))
    city = _clean_geocodio_text(row.get("AddressCity", ""))
    zip_code = _clean_geocodio_zip(row.get("AddressZip", ""))

    street_parts = [p for p in [line1, line2] if p]
    locality_parts = [p for p in [city, "MO"] if p]
    if zip_code:
        locality_parts.append(zip_code)
    if not street_parts and not locality_parts:
        return ""
    parts = []
    if street_parts:
        parts.append(" ".join(street_parts))
    if locality_parts:
        parts.append(", ".join(locality_parts))
    return ", ".join(parts)


def _extract_geocodio_result(result: dict, fields: List[str]) -> dict:
    payload = {
        "geocodio_latitude": None,
        "geocodio_longitude": None,
        "geocodio_accuracy": None,
        "geocodio_accuracy_type": None,
        "geocodio_formatted_address": None,
    }
    if not result:
        return payload
    location = result.get("location") or {}
    payload["geocodio_latitude"] = location.get("lat")
    payload["geocodio_longitude"] = location.get("lng")
    payload["geocodio_accuracy"] = result.get("accuracy")
    payload["geocodio_accuracy_type"] = result.get("accuracy_type")
    payload["geocodio_formatted_address"] = result.get("formatted_address")

    field_map = result.get("fields") or {}
    for field in fields:
        col = f"geocodio_{field.replace('-', '_')}"
        payload[col] = field_map.get(field)
    payload["geocodio_fields"] = field_map or None
    return payload


def _ensure_geocodio_columns(df: pd.DataFrame, fields: List[str]) -> pd.DataFrame:
    base_cols = [
        "geocodio_latitude",
        "geocodio_longitude",
        "geocodio_accuracy",
        "geocodio_accuracy_type",
        "geocodio_formatted_address",
        "geocodio_fields",
        "geocodio_error",
        "geocodio_query",
    ]
    for col in base_cols:
        if col not in df.columns:
            df[col] = pd.NA
    for field in fields:
        col = f"geocodio_{field.replace('-', '_')}"
        if col not in df.columns:
            df[col] = pd.NA
    return df


@op(out=Out(pd.DataFrame), required_resource_keys={"data_dir_processed"})
def geocode_agency_list(context, agency_list: pd.DataFrame) -> pd.DataFrame:
    """Geocode agency addresses and append Geocodio fields."""
    out_dir = Path(context.resources.data_dir_processed.get_path())
    out_path = out_dir / "agency_list_geocoded.parquet"

    if agency_list.empty:
        context.log.warning("Agency list is empty; writing empty geocoded output.")
        agency_list = _ensure_geocodio_columns(agency_list.copy(), GEOCODIO_FIELDS)
        agency_list.to_parquet(out_path, index=False, engine="pyarrow")
        return agency_list

    api_key = os.getenv("GEOCODIO_API_KEY")
    if not api_key:
        context.log.warning("GEOCODIO_API_KEY not set; returning agency list without geocoding.")
        agency_list = _ensure_geocodio_columns(agency_list.copy(), GEOCODIO_FIELDS)
        agency_list.to_parquet(out_path, index=False, engine="pyarrow")
        return agency_list

    try:
        from geocodio import Geocodio
    except ImportError as exc:
        raise ImportError("geocodio package is required for geocoding (uv add geocodio).") from exc

    geocode_queries = []
    geocode_indices = []
    query_col = "geocodio_query"
    working = agency_list.copy()
    working[query_col] = working.apply(_build_geocodio_query, axis=1)
    for idx, query in working[query_col].items():
        if query:
            geocode_indices.append(idx)
            geocode_queries.append(query)
        else:
            working.loc[idx, "geocodio_error"] = "missing_address"

    client = Geocodio(api_key)
    batch_size = _env_positive_int("GEOCODIO_BATCH_SIZE", 100)
    fields = GEOCODIO_FIELDS

    for start in range(0, len(geocode_queries), batch_size):
        batch_queries = geocode_queries[start : start + batch_size]
        batch_indices = geocode_indices[start : start + batch_size]
        try:
            response = client.geocode(batch_queries, fields=fields)
        except Exception as exc:
            context.log.exception("Geocodio batch failed (%d-%d): %s", start, start + len(batch_queries) - 1, exc)
            for idx in batch_indices:
                working.loc[idx, "geocodio_error"] = str(exc)
            continue

        responses = response if isinstance(response, list) else [response]
        if len(responses) != len(batch_indices):
            context.log.warning(
                "Geocodio response count mismatch (expected %d, got %d).",
                len(batch_indices),
                len(responses),
            )

        for idx, item in zip(batch_indices, responses):
            results = item.get("results") if isinstance(item, dict) else None
            if not results:
                working.loc[idx, "geocodio_error"] = (item.get("error") if isinstance(item, dict) else "no_results")
                continue
            payload = _extract_geocodio_result(results[0], fields)
            for key, value in payload.items():
                working.loc[idx, key] = value

    working = _ensure_geocodio_columns(working, fields)
    working.to_parquet(out_path, index=False, engine="pyarrow")
    context.log.info("Wrote geocoded agency list Parquet → %s (%d rows)", out_path, len(working))
    try:
        context.add_output_metadata({"local_path": str(out_path), "row_count": len(working)})
    except Exception:
        pass
    return working


@graph_asset(
    name="agency_list_geocoded",
    group_name="agency_reference",
    ins={"agency_list": AssetIn(key=AssetKey("agency_list"))},
    description="Geocode agency addresses and append Geocodio fields.",
)
def agency_list_geocoded_asset(agency_list: pd.DataFrame) -> pd.DataFrame:
    return geocode_agency_list(agency_list)

# ------------------------------------------------------------------------------
# Agency reference (crosswalk applied)
# ------------------------------------------------------------------------------
@op(out=Out(pd.DataFrame), required_resource_keys={"data_dir_source", "data_dir_processed"})
def build_agency_reference(context, agency_list_geocoded: pd.DataFrame) -> pd.DataFrame:
    """Join agency metadata to crosswalk and emit a canonical Department name."""
    from missouri_vsr.cli_crosswalk import _normalize_name

    crosswalk_path = Path(context.resources.data_dir_source.get_path()) / "agency_crosswalk.csv"
    if crosswalk_path.exists():
        crosswalk = pd.read_csv(crosswalk_path)
        context.log.info("Loaded crosswalk CSV → %s (%d rows)", crosswalk_path, len(crosswalk))
    else:
        context.log.warning("Crosswalk CSV not found at %s; output will have blank Canonical.", crosswalk_path)
        crosswalk = pd.DataFrame(columns=["Normalized", "Raw", "Canonical"])

    if agency_list_geocoded.empty:
        context.log.warning("Agency list is empty; writing empty agency reference.")
        merged = agency_list_geocoded.copy()
        merged["Normalized"] = pd.NA
        merged["Canonical"] = pd.NA
    else:
        name_col = next(
            (c for c in agency_list_geocoded.columns if str(c).lower() in {"department", "agency", "name"}),
            agency_list_geocoded.columns[0],
        )
        context.log.info("Using agency name column for crosswalk: %s", name_col)
        merged = agency_list_geocoded.copy()
        merged["Normalized"] = merged[name_col].fillna("").astype(str).apply(_normalize_name)

        if not crosswalk.empty:
            crosswalk = crosswalk.copy()
            expected_cols = {"Normalized", "Canonical"}
            if not expected_cols.issubset(crosswalk.columns):
                context.log.warning(
                    "Crosswalk CSV missing expected columns %s; skipping join.",
                    sorted(expected_cols - set(crosswalk.columns)),
                )
                crosswalk = pd.DataFrame(columns=["Normalized", "Raw", "Canonical"])
            crosswalk["Normalized"] = crosswalk["Normalized"].astype(str)
            crosswalk["Canonical"] = crosswalk["Canonical"].where(pd.notna(crosswalk["Canonical"]), "")
            crosswalk["Canonical"] = crosswalk["Canonical"].astype(str).str.strip()
            crosswalk = crosswalk[crosswalk["Normalized"].ne("")]
            dup_mask = crosswalk.duplicated(subset=["Normalized"], keep=False)
            if dup_mask.any():
                dupe_sample = (
                    crosswalk.loc[dup_mask, "Normalized"]
                    .drop_duplicates()
                    .head(5)
                    .tolist()
                )
                context.log.warning(
                    "Crosswalk has duplicate Normalized values; keeping first. Sample: %s",
                    dupe_sample,
                )
            crosswalk = crosswalk.drop_duplicates(subset=["Normalized"], keep="first")
            merged = merged.merge(crosswalk, on="Normalized", how="left", suffixes=("", "_cw"))

        canonical_col = None
        if "Canonical" in merged.columns:
            canonical_col = "Canonical"
        elif "Canonical_cw" in merged.columns:
            canonical_col = "Canonical_cw"
        if canonical_col is None:
            merged["Canonical"] = pd.NA
        else:
            merged["Canonical"] = merged[canonical_col].replace({"": pd.NA, "nan": pd.NA, "NaN": pd.NA})

    merged["Department"] = merged["Canonical"]

    out_dir = Path(context.resources.data_dir_processed.get_path())
    out_path = out_dir / "agency_reference.parquet"
    merged.to_parquet(out_path, index=False, engine="pyarrow")
    context.log.info("Wrote agency reference Parquet → %s (%d rows)", out_path, len(merged))
    try:
        context.add_output_metadata({"local_path": str(out_path), "row_count": len(merged)})
    except Exception:
        pass
    return merged


@graph_asset(
    name="agency_reference",
    group_name="processed",
    ins={"agency_list_geocoded": AssetIn(key=AssetKey("agency_list_geocoded"))},
    description="Join agency list metadata to the crosswalk and output canonical Department names.",
)
def agency_reference_asset(agency_list_geocoded: pd.DataFrame) -> pd.DataFrame:
    return build_agency_reference(agency_list_geocoded)
