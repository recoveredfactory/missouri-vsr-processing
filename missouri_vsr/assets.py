from __future__ import annotations

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
}

# ------------------------------------------------------------------------------
# Download PDFs (one logical asset per year)
# ------------------------------------------------------------------------------
@multi_asset(
    outs={str(year): AssetOut(metadata={"url": url}) for year, url in YEAR_URLS.items()},
    group_name="vsr_reports",
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

    if USE_EXAMPLES:
        context.log.info("VSR_USE_EXAMPLES=1 → using local example PDFs; skipping downloads")
        # Restrict selection to years for which we have examples
        available = {str(y) for y in EXAMPLE_PDFS.keys()}
        if selected - available:
            context.log.warning(
                "Examples available only for %s; will skip %s",
                sorted(available),
                sorted(selected - available),
            )
        selected = selected & available

    for year, url in YEAR_URLS.items():
        name = str(year)
        if name not in selected:
            context.log.debug("Skipping %s (not selected)", name)
            continue

        # In example mode, just yield the example file path
        if USE_EXAMPLES and year in EXAMPLE_PDFS:
            example_path = EXAMPLE_PDFS[year]
            if not example_path.exists():
                raise FileNotFoundError(f"Missing example PDF: {example_path}")
            context.log.info("Using example PDF for %s → %s", year, example_path)
            yield Output(
                str(example_path),
                output_name=name,
                metadata={"mode": "examples", "local_path": str(example_path)},
            )
            continue

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
    "section",
]
NUMERIC_COLS = FINAL_COLUMNS[1:8]

TABLE_SLUG_LOOKUP = {
    "Rates by Race": "rates",
    "Number of Stops by Race": "stops",
    "Search statistics": "search",
}

# Optional toggle to use small example PDFs instead of full reports.
# Set env var `VSR_USE_EXAMPLES=1` to enable.
USE_EXAMPLES = os.getenv("VSR_USE_EXAMPLES", "").strip().lower() in {"1", "true", "yes", "on"}

# Map years to local example PDFs (keep light-weight for dev/testing)
EXAMPLE_PDFS: dict[int, Path] = {
    2024: Path("data/src/examples/Example-StL-VSReport2024.pdf"),
}
TABLE_SECTIONS: dict[str, list[str]] = {
    "rates": ["Population", "Totals", "Rates"],
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
    "search": [
        "Probable cause",
        "What searched",
        "Search duration",
        "Contraband found",
    ],
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
        if tok == '.' and i + 1 < len(row) and row[i + 1].isdigit():
            merged.append(f"0.{row[i + 1]}")
            i += 2
            continue
        if i + 2 < len(row) and row[i + 1] == '.' and row[i].isdigit() and row[i + 2].isdigit():
            merged.append(f"{row[i]}.{row[i + 2]}")
            i += 3
            continue
        merged.append(tok)
        i += 1
    # Remove dot-leader filler tokens (.., ..., ···) after merging
    row = [t for t in merged if not _DOTS_RE.match(t)]

    # Pick the rightmost 7 numeric tokens from the row; everything else is key
    numeric_positions = [idx for idx, tok in enumerate(row) if _is_numeric(tok)]
    picked_positions = numeric_positions[-7:]
    numeric = [row[idx] for idx in picked_positions]

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

    # Generic salvage for rows that imply rates/percentages but lost decimals in PDF text
    key_l = key.lower()
    if ("rate" in key_l) or ("%" in key_l):
        fixed: list[str] = []
        is_residents = "residents" in key_l
        for s in numeric:
            if not s:
                fixed.append("")
                continue
            raw = s.replace(",", "")
            if raw.endswith('%'):
                raw = raw[:-1]
            if "." in raw or raw == "0":
                fixed.append(raw)
                continue
            if raw.isdigit() and raw != "100":
                if len(raw) == 4 and '%' in key_l:
                    fixed.append(str(int(raw) / 100.0))
                elif len(raw) == 3:
                    fixed.append(str(int(raw) / 100.0))
                elif len(raw) == 2:
                    fixed.append(str(int(raw) / (100.0 if is_residents else 10.0)))
                else:
                    fixed.append(raw)
            else:
                fixed.append(raw)
        numeric = fixed

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

    # Build normalized token rows from text-only df; keep a flag for numeric presence.
    has_numeric_flags: List[bool] = []
    normalized_rows: List[List[str]] = []
    for _, row in df.iterrows():
        toks = [str(c).strip() for c in row if str(c).strip()]
        has_numeric = any(_is_numeric(t) for t in toks)
        has_numeric_flags.append(has_numeric)
        normalized_rows.append(
            normalize_row_tokens(list(toks), dept_name, table_slug, log)
        )
    is_section_row = [not b for b in has_numeric_flags]
    df = pd.DataFrame(normalized_rows, columns=FINAL_COLUMNS[:8])

    df["key"] = (
        df["key"]
        .astype(str)
        .str.replace(r"\s*\n\s*", " ", regex=True)
        .str.strip()
        .str.lower()
        .apply(_normalize_text)
    )

    # Attach sections: generic rule → rows without numbers are section headers, then ffill
    df["section"] = None
    for i, is_section in enumerate(is_section_row):
        if is_section:
            df.loc[i, "section"] = df.loc[i, "key"].strip()
    section_header_mask = pd.Series(is_section_row, index=df.index)
    
    df["section"] = df["section"].ffill()

    # Remove blank rows, notes, and the section-header rows themselves
    mask_blank_key = df["key"].str.strip().eq("") | df["key"].isna()
    mask_notes = df["key"].str.contains(r"^notes?\s*:\s*", case=False, na=False)
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
        if row.section:
            parts.extend(["", slugify(str(row.section), lowercase=True, replacements=[["%", "pct"]]), ""])
        parts.append(slugify(str(row.key), lowercase=True, replacements=[["%", "pct"]]))
        return "-".join(parts)

    df["slug"] = df.apply(_build_slug, axis=1)
    df["department"] = dept_name
    df["table name"] = raw_table_name
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
def concat_and_write_json(context, chunks: List[pd.DataFrame], pdf_path: str) -> pd.DataFrame:
    """Merge page chunks, write JSON, return combined DataFrame."""
    non_empty = [c for c in chunks if not c.empty]
    if not non_empty:
        raise ValueError("No tables were extracted from the PDF.")
    combined = pd.concat(non_empty, ignore_index=True)

    year_match = re.search(r"(\d{4})", Path(pdf_path).name)
    year = year_match.group(1) if year_match else "unknown"

    out_json = context.resources.data_dir_processed.get_path() / f"combined_output_{year}.json"
    combined.to_json(out_json, index=False, orient="records", default_handler=str)
    context.log.info("Wrote %d rows → %s", len(combined), out_json)
    return combined

# ------------------------------------------------------------------------------
# Extract graph assets factory
# ------------------------------------------------------------------------------
def make_extract_asset(year: int):
    @graph_asset(
        name=f"extract_pdf_data_{year}",
        group_name="vsr_extract",
        ins={"pdf_path": AssetIn(key=AssetKey(str(year)))},
        description=f"Extract tabular data from the {year} VSR (parallelised per page range).",
    )
    def extract_for_year(pdf_path: str) -> pd.DataFrame:
        page_ranges = calculate_page_ranges(pdf_path)
        processed = page_ranges.map(lambda pr: parse_page_range(pdf_path, pr))
        return concat_and_write_json(processed.collect(), pdf_path)

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
    required_resource_keys={"data_dir_processed"},
)
def combine_reports(context, **extracted_reports: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Concatenate all extract_pdf_data_* assets into a single DataFrame and write combined JSON."""
    # Merge all DataFrames
    dfs = [df for df in extracted_reports.values() if not df.empty]
    if not dfs:
        raise ValueError("No extracted tables found to combine.")
    combined = pd.concat(dfs, ignore_index=True)

    # Write combined JSON
    processed_dir = Path(context.resources.data_dir_processed.get_path())
    out_file = processed_dir / "all_combined_output.json"
    combined.to_json(out_file, orient="records", default_handler=str)
    context.log.info("Wrote combined JSON: %d rows → %s", len(combined), out_file)
    return combined

@graph_asset(
    name="combine_all_reports",
    group_name="vsr_processed",
    ins={
        f"extract_pdf_data_{year}": AssetIn(key=AssetKey(f"extract_pdf_data_{year}"))
        for year in YEAR_URLS
    },
    description="Combine all per-year extract_pdf_data_* assets into a single JSON and DataFrame."
)
def combine_all_reports(**extracted_reports: pd.DataFrame) -> pd.DataFrame:
    return combine_reports(**extracted_reports)
