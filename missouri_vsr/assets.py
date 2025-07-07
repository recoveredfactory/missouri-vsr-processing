"""
Dagster asset that extracts VSR PDF tables into friendly, tidy JSON.
"""

import re
import unicodedata
from typing import List

import camelot
import pandas as pd
from PyPDF2 import PdfReader
from slugify import slugify  
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
    "section",
]

NUMERIC_COLS = FINAL_COLUMNS[1:8]  

TABLE_SLUG_LOOKUP = {
    "Rates by Race": "rates",
    "Number of Stops by Race": "stops",
    "Search statistics": "search",
}

TABLE_SECTIONS: dict[str, list[str]] = {
    # Table: "Table 1 – Population, Totals & Rates" in the PDFs
    "rates": [
        "Population",
        "Totals",
        "Rates",
    ],
    # Table: "Table 2 – Stops"
    "stops": [
        "All Stops",
        "Reason for Stop",
        "Stop Outcome",
        "Citation/warning violation",
        "Arrest violation",
        "Officer Assignment",
        "Location of Stop",
        "Driver Gender",
        "Driver Age",
    ],
    # Table: "Table 3 – Searches"
    "search": [
        "Probable cause",
        "What searched",
        "Search duration",
        "Contraband found",
    ],
}

# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------

def _normalize_text(text: str) -> str:
    """Strip ligatures & smart quotes to plain ASCII."""
    text = unicodedata.normalize("NFKC", text)
    return text.replace("ﬀ", "ff").replace("’", "'")



# ----------------------------------------------------------------------------
# Table‑level cleanup
# ----------------------------------------------------------------------------

_NUM_RE = re.compile(r"^\d[\d,\.]*$")  # accepts “.” later

def _is_numeric(tok: str) -> bool:
    tok = tok.strip()
    return tok == "." or bool(re.match(r"^\d[\d,\.]*$", tok))

def normalize_row_tokens(row: list[str], dept_name: str, table_slug: str, log) -> list[str]:
    log.debug("Raw row tokens: %s / %s %s", list(row), table_slug, dept_name)
    
    row = [str(c).strip() for c in row if str(c).strip()]

    # Pull out the last 7 numeric-looking tokens
    numeric = []
    while row and len(numeric) < 7 and _is_numeric(row[-1]):
        numeric.insert(0, row.pop())          # build in normal order
    if len(numeric) < 7:
        log.warning("Found only %d numeric cols: %s", len(numeric), numeric)
        numeric = [""] * (7 - len(numeric)) + numeric
    elif len(numeric) > 7:
        log.debug("Extra numeric cols %s – keeping right-most 7", numeric[:-7])
        numeric = numeric[-7:]

    key = " ".join(row).strip()
    if not key:
        key = "(blank key)"
    return [key] + numeric

def _clean_camelot_table(table, log) -> pd.DataFrame | None:
    """Return a tidy :class:`~pandas.DataFrame` for one Camelot table.

    If the table cannot be parsed it returns *None* so that callers
    can safely ``continue`` when iterating over tables extracted from
    a page.

    The heuristics here are tailored for Missouri Attorney General
    traffic‑stop reports.  See :data:`TABLE_SECTIONS` for the known
    section structure of each table.
    """

    # ---------------------- initial cleanup ----------------------
    df = table.df.copy()
    log.debug("Raw Camelot table shape: %s", df.shape)

    # drop completely empty columns created by Camelot's lattice mode
    df = df.dropna(axis=1, how="all")
    log.debug("Shape after dropping empty cols: %s", df.shape)

    # ---------------------- metadata row ----------------------
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
        log.debug("Missing metadata row / Raw table:\n%s", df.to_string())
        return None

    m = re.search(r"Table\s*(\d+):\s*(.*?)\s*for\s*(.+)", metadata_line)
    if not m:
        log.warning("Could not parse metadata line: %s", metadata_line)
        log.debug("Could not parse meta data line / Raw table:\n%s", df.to_string())
        return None

    raw_table_name, raw_dept = (
        m.group(2).strip(),
        m.group(3).strip(),
    )

    dept_name = _normalize_text(raw_dept)
    table_slug = TABLE_SLUG_LOOKUP.get(
        raw_table_name, slugify(raw_table_name, lowercase=True)
    )

    if table_slug == "stops":
        log.debug("Raw table for stops in %s:\n%s", dept_name, df.head(100).to_string())

    log.info("Parsed meta → table '%s' / dept '%s'", raw_table_name, dept_name)

    # ---------------------- body rows ----------------------
    df = df.iloc[metadata_row_idx + 2 :].reset_index(drop=True)

    # If Camelot produced a blank first column, shift left so we always
    # have the expected 8 columns.
    if pd.isna(df.iloc[0, 0]) or str(df.iloc[0, 0]).strip() == "":
        df = df.iloc[:, 1:]

    if table_slug == "stops":
        log.debug("Cleaned up table for stops in %s:\n%s", dept_name, df.head(100).to_string())

    # Try to fix malformed rows using knowledge of table structure before trusting column values
    normalized_rows = []
    for _, row in df.iterrows():
        normalized_rows.append(normalize_row_tokens(list(row), dept_name, table_slug, log))

    df = pd.DataFrame(normalized_rows, columns=FINAL_COLUMNS[:8])

    if table_slug == "stops":
        log.debug("Final cleaned up table before section detection")
        log.debug(df.head(50))

    # ---------------------- section detection ----------------------
    # Normalise the *key* column once so we can safely compare values.
    df["key"] = (
        df["key"].astype(str)
            .str.replace(r"\s*\n\s*", " ", regex=True)
            .str.strip()
            .str.lower()
            .apply(_normalize_text)
    )

    # Lookup the list of expected section names for this table.  If the
    # table is unknown we fall back to empty list and behave as before.
    table_sections = TABLE_SECTIONS.get(table_slug, [])
    section_lookup = {s.lower(): s for s in table_sections}

    mask_section_row = df["key"].map(section_lookup.__contains__).fillna(False)

    # Store the *canonical* section name (from our lookup) then ffill so
    # that every body row is tagged with the section it belongs to.
    df["section"] = None
    df.loc[mask_section_row, "section"] = df.loc[mask_section_row, "key"].map(section_lookup)
    df["section"] = df["section"].ffill()

    # ---------------------- drop non‑data rows ----------------------
    mask_blank_key = df["key"].isna() | (df["key"].str.strip() == "")
    mask_notes = df["key"].str.contains(r"^Notes?:", case=False, na=False)

    if table_slug == "stops":
        log.debug("Rows before dropping blanks/notes/section headers")
        log.debug(df.head(50))

    # *Only* drop the rows we just identified as section headers (they
    # are purely metadata) plus blanks/notes.  Unlike the old logic we
    # **keep** rows whose numeric columns are all "." because they may be
    # legitimate zero‑count data rows.
    df = df[~(mask_blank_key | mask_notes | mask_section_row)].copy()
    if df.empty:
        log.warning("All rows removed after cleanup – skipping table")
        return None

    # ---------------------- numeric coercion ----------------------
    # Treat solitary dots as missing values, then coerce the remainder
    # to numeric.  Any stray non‑numeric strings become NaN.
    df[NUMERIC_COLS] = df[NUMERIC_COLS].replace(".", pd.NA)
    df[NUMERIC_COLS] = df[NUMERIC_COLS].apply(
        lambda col: pd.to_numeric(col, errors="coerce")
    )

    # ---------------------- slugs & metadata ----------------------
    def _build_slug(row: pd.Series) -> str:
        parts: list[str] = [table_slug]
        if row.section:
            parts.append("")  # preserve double hyphens for readability
            parts.append(
                slugify(
                    str(row.section), lowercase=True, replacements=[["%", "pct"]]
                )
            )
            parts.append("")
        parts.append(
            slugify(str(row.key), lowercase=True, replacements=[["%", "pct"]])
        )
        return "-".join(parts)

    df["slug"] = df.apply(_build_slug, axis=1)
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
    total_pages = min(total_pages, 25)

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
        # tables = camelot.read_pdf(pdf_file, pages=page_range, flavor="stream", edge_tol=500, row_tol=10)
        tables = camelot.read_pdf(
            pdf_file,
            pages=page_range,
            flavor="stream",
            edge_tol=50,
            row_tol=0
        )

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


@graph_asset(
    description="Extract tabular data from a Vehicle Stops Report PDF (graph-backed to allow for Dagster-native paralellization).",
)
def extract_pdf_data():
    ranges = calculate_page_ranges()
    chunks = ranges.map(parse_page_range)
    return concat_and_write_json(chunks.collect())
