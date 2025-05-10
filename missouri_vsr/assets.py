"""Dagster asset using *dynamic mapping* instead of a manual `ProcessPoolExecutor`.

Compatible with **dagster>=1.10.5,<2**.

Key points
-----------
* **`calculate_page_ranges`** emits one `DynamicOutput[str]` per page‑range.
* **`parse_page_range`** is *mapped* over that dynamic output and itself returns a
  `DynamicOutput[pd.DataFrame]` (one per page‑range), which keeps the execution
  graph fully dynamic.
* **`concat_and_write_csv`** receives the *collected* list of DataFrames and
  writes the combined CSV.

This pattern preserves per‑range parallelism, streams logs naturally to Dagster,
**and uses only APIs present in 1.10.x**.
"""

from __future__ import annotations

import re
from typing import List

import camelot
import pandas as pd
from PyPDF2 import PdfReader
from dagster import (
    AssetExecutionContext,
    DynamicOut,
    DynamicOutput,
    Out,
    graph_asset,
    op,
)

PAGE_CHUNK_SIZE = 5  # tune as needed

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

# ----------------------------------------------------------------------------
# Helper – table cleanup (pure function, importable by tests)
# ----------------------------------------------------------------------------

def _clean_camelot_table(table) -> pd.DataFrame | None:  # noqa: ANN001
    """Return a tidy DataFrame for one Camelot table or *None* if unusable."""

    df = table.df.copy()

    # Drop entirely empty columns.
    df = df.dropna(axis=1, how="all")

    # Find metadata row – e.g. "Table 1: Rates by Race for Adair County ..."
    metadata_row_idx: int | None = None
    metadata_line: str | None = None
    for i, row in df.iterrows():
        line = " ".join(row.dropna())
        if re.search(r"Table\s*\d+:", line):
            metadata_row_idx, metadata_line = i, line
            break
    if metadata_row_idx is None or metadata_line is None:
        return None

    m = re.search(r"Table\s*(\d+):\s*(.*?)\s*for\s*(.+)", metadata_line)
    if not m:
        return None

    _table_number, table_name, dept_name = (
        m.group(1),
        m.group(2).strip(),
        m.group(3).strip(),
    )

    # Drop metadata + header rows
    df = df.iloc[metadata_row_idx + 2 :].reset_index(drop=True)

    # If first cell is blank shift left
    if pd.isna(df.iloc[0, 0]) or str(df.iloc[0, 0]).strip() == "":
        df = df.iloc[:, 1:]

    if df.shape[1] < 8:
        return None

    df = df.iloc[:, :8]
    df.columns = FINAL_COLUMNS[:8]
    df["department"] = dept_name
    df["table name"] = table_name
    return df

# ----------------------------------------------------------------------------
# Ops
# ----------------------------------------------------------------------------

@op(
    out=DynamicOut(str),
    required_resource_keys={"data_dir_report_pdfs"},
)
def calculate_page_ranges(context):  # noqa: D401, ANN001
    """Yield *one* `DynamicOutput[str]` per page‑range so downstream ops can be mapped."""

    pdf_file = context.resources.data_dir_report_pdfs.get_path() / "VSRreport2023.pdf"
    total_pages = len(PdfReader(pdf_file).pages)

    # Temp: keep runs shorter while iterating
    total_pages = min(total_pages, 100)

    for i in range(1, total_pages + 1, PAGE_CHUNK_SIZE):
        page_range = f"{i}-{min(i + PAGE_CHUNK_SIZE - 1, total_pages)}"
        context.log.debug("Emitting page‑range %s", page_range)
        # Mapping keys must be valid python identifiers → replace dash with underscore
        yield DynamicOutput(page_range, mapping_key=page_range.replace("-", "_"))


@op(
    out=Out(pd.DataFrame),
    required_resource_keys={"data_dir_report_pdfs"},
)
def parse_page_range(context, page_range: str):  # noqa: D401
    """Parse one page‑range and emit a tidy DataFrame (or empty df if nothing usable)."""

    pdf_file = context.resources.data_dir_report_pdfs.get_path() / "VSRreport2023.pdf"
    context.log.info("Parsing pages %s", page_range)

    try:
        tables = camelot.read_pdf(pdf_file, pages=page_range, flavor="stream")
    except Exception as exc:  # pragma: no cover – surface in logs
        context.log.error("Camelot failed on %s: %s", page_range, exc)
        yield DynamicOutput(pd.DataFrame(), mapping_key=page_range.replace("-", "_"))
        return

    dfs = filter(None, (_clean_camelot_table(t) for t in tables))
    combined = pd.concat(list(dfs), ignore_index=True) if tables else pd.DataFrame()
    if combined.empty:
        context.log.warning("No usable tables on pages %s", page_range)
    return combined


@op(out=Out(pd.DataFrame), required_resource_keys={"data_dir_processed"})
def concat_and_write_csv(context, chunks: List[pd.DataFrame]) -> pd.DataFrame:  # noqa: D401
    non_empty = [c for c in chunks if not c.empty]
    if not non_empty:
        raise ValueError("No tables were extracted from the PDF.")

    combined = pd.concat(non_empty, ignore_index=True)
    out_csv = context.resources.data_dir_processed.get_path() / "combined_output.csv"
    combined.to_csv(out_csv, index=False)
    context.log.info("Wrote %d rows → %s", len(combined), out_csv)
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
