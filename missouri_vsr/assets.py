"""Dagster asset that extracts VSR PDF tables via **dynamic mapping**.

* Compatible with **dagster>=1.10.5,<2**.
* Preserves **fine‑grained logging** inside the table‑cleanup helper so you can
  debug malformed tables right from the Dagster UI.
* **Recent upgrades**
    * Config‑driven filename (default → *VSRreport2023.pdf*).
    * Robust cleanup: blanks, “Notes” rows, ligatures, smart quotes, dot‑only rows.
    * Slug column `<table>-<key>` for stable joins.
    * **NEW (numeric patch)**
        • Numeric columns coercively parsed to numbers; “.” becomes *null*.
        • JSON output now emits unquoted numeric values for totals by race.
"""

from __future__ import annotations

import re
import unicodedata
from typing import List

import camelot
import pandas as pd
from PyPDF2 import PdfReader
from slugify import slugify  # pip install python-slugify
from dagster import (
    DynamicOut,
    DynamicOutput,
    Field,
    Out,
    graph_asset,
    op,
)

PAGE_CHUNK_SIZE = 5  # tune as needed
DEFAULT_PDF_FILENAME = "VSRreport2023.pdf"

FINAL_COLUMNS = [
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

TABLE_SLUG_LOOKUP = {
    "Rates by Race": "rates",
    "Number of Stops by Race": "stops",
    "Search statistics": "search",
}

# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------

def _normalize_text(text: str) -> str:
    """Strip ligatures & smart quotes to plain ASCII."""

    text = unicodedata.normalize("NFKC", text)
    return text.replace("ﬀ", "ff").replace("’", "'")


_NUMERIC_COLS = FINAL_COLUMNS[1:8]  # cached once for speed

# ----------------------------------------------------------------------------
# Table‑level cleanup
# ----------------------------------------------------------------------------

def _clean_camelot_table(table, log) -> pd.DataFrame | None: 
    """Return a tidy DataFrame for one Camelot table or *None* if unusable."""

    df = table.df.copy()
    log.debug("Raw Camelot table shape: %s", df.shape)

    # Drop all‑NaN columns early.
    df = df.dropna(axis=1, how="all")
    log.debug("Shape after dropping empty cols: %s", df.shape)

    # ---------------------- metadata ----------------------
    metadata_row_idx: int | None = None
    metadata_line: str | None = None
    for i, row in df.iterrows():
        line = " ".join(row.dropna())
        if re.search(r"Table\s*\d+:", line):
            metadata_row_idx, metadata_line = i, line
            log.debug("Metadata row %d → %s", i, line)
            break
    if metadata_row_idx is None:
        log.warning("Missing metadata row – skipping table")
        return None

    m = re.search(r"Table\s*(\d+):\s*(.*?)\s*for\s*(.+)", metadata_line)
    if not m:
        log.warning("Could not parse metadata line: %s", metadata_line)
        return None

    _tbl_num, raw_table_name, raw_dept = (
        m.group(1),
        m.group(2).strip(),
        m.group(3).strip(),
    )

    dept_name = _normalize_text(raw_dept)
    table_slug = TABLE_SLUG_LOOKUP.get(raw_table_name, slugify(raw_table_name, lowercase=True))

    log.info("Parsed meta → table '%s' / dept '%s'", raw_table_name, dept_name)

    # ---------------------- body rows ----------------------
    df = df.iloc[metadata_row_idx + 2 :].reset_index(drop=True)

    # If first cell blank shift left for consistent 8 cols.
    if pd.isna(df.iloc[0, 0]) or str(df.iloc[0, 0]).strip() == "":
        df = df.iloc[:, 1:]

    if df.shape[1] < 8:
        log.warning("Unexpected column count %d – skipping table", df.shape[1])
        return None

    df = df.iloc[:, :8]
    df.columns = FINAL_COLUMNS[:8]

    # ---------------------- row‑level cleanup ----------------------
    df["key"] = df["key"].astype(str).str.replace(r"\s*\n\s*", " ", regex=True).str.strip()

    mask_blank_key = df["key"].isna() | (df["key"].str.strip() == "")
    mask_notes = df["key"].str.contains(r"^Notes?:", case=False, na=False)
    mask_all_dots = df[_NUMERIC_COLS].applymap(lambda x: str(x).strip() == ".").all(axis=1)

    df = df[~(mask_blank_key | mask_notes | mask_all_dots)].copy()

    if df.empty:
        log.warning("All rows removed after cleanup – skipping table")
        return None

    # ---------------------- numeric coercion ----------------------
    df[_NUMERIC_COLS] = df[_NUMERIC_COLS].replace(".", pd.NA)
    df[_NUMERIC_COLS] = df[_NUMERIC_COLS].apply(lambda col: pd.to_numeric(col, errors="coerce"))

    # ---------------------- slugs & metadata ----------------------
    df["slug"] = df["key"].apply(lambda k: f"{table_slug}-{slugify(str(k), lowercase=True)}")
    df["department"] = dept_name
    df["table name"] = raw_table_name

    log.debug("Final tidy table shape: %s", df.shape)
    return df

# ----------------------------------------------------------------------------
# Ops
# ----------------------------------------------------------------------------

_CFG = {
    "pdf_filename": Field(
        str,
        default_value=DEFAULT_PDF_FILENAME,
        description="PDF to parse (relative to resources.data_dir_report_pdfs)",
    )
}

@op(
    out=DynamicOut(str),
    required_resource_keys={"data_dir_report_pdfs"},
    config_schema=_CFG,
)
def calculate_page_ranges(context):  
    pdf_file = context.resources.data_dir_report_pdfs.get_path() / context.op_config["pdf_filename"]
    total_pages = len(PdfReader(pdf_file).pages)
    total_pages = min(total_pages, 100)

    for i in range(1, total_pages + 1, PAGE_CHUNK_SIZE):
        page_range = f"{i}-{min(i + PAGE_CHUNK_SIZE - 1, total_pages)}"
        yield DynamicOutput(page_range, mapping_key=page_range.replace("-", "_"))


@op(
    out=Out(pd.DataFrame),
    required_resource_keys={"data_dir_report_pdfs"},
    config_schema=_CFG,
)
def parse_page_range(context, page_range: str) -> pd.DataFrame:  
    pdf_file = context.resources.data_dir_report_pdfs.get_path() / context.op_config["pdf_filename"]

    try:
        tables = camelot.read_pdf(pdf_file, pages=page_range, flavor="stream")
    except Exception as exc:
        context.log.error("Camelot failed on %s: %s", page_range, exc)
        return pd.DataFrame()

    frames = [
        cleaned
        for t in tables
        if (cleaned := _clean_camelot_table(t, context.log)) is not None and not cleaned.empty
    ]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


@op(out=Out(pd.DataFrame), required_resource_keys={"data_dir_processed"})
def concat_and_write_json(context, chunks: List[pd.DataFrame]) -> pd.DataFrame:  
    non_empty = [c for c in chunks if not c.empty]
    if not non_empty:
        raise ValueError("No tables were extracted from the PDF.")

    combined = pd.concat(non_empty, ignore_index=True)
    out_json = context.resources.data_dir_processed.get_path() / "combined_output.json"
    combined.to_json(out_json, index=False, orient="records", default_handler=str)
    context.log.info("Wrote %d rows → %s", len(combined), out_json)
    return combined


# ----------------------------------------------------------------------------
# Asset (graph‑backed)
# ----------------------------------------------------------------------------

@graph_asset(
    description=(
        "Extracts tabular data from the annual VSR PDF via dynamic mapping; "
        "each page‑range is processed in its own Dagster op, so logs stream naturally."
    )
)
def extract_pdf_data():  # noqa: D401
    page_ranges = calculate_page_ranges()            # DynamicOutput[str]
    chunk_dfs = page_ranges.map(parse_page_range)    # dynamic mapping via .map on DynamicOutput
    return concat_and_write_csv(chunk_dfs.collect())
