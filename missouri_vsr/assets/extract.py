from __future__ import annotations

import os
import re
import subprocess
import unicodedata
from pathlib import Path
from typing import Dict, List

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
EXTRACT_COLUMNS = [
    "Total",
    "White",
    "Black",
    "Hispanic",
    "Native American",
    "Asian",
    "Other",
    "year",
    "slug",
    "agency",
    "table",
    "table_id",
    "section",
    "section_id",
    "metric",
    "metric_id",
    "row_id",
]

TABLE_SLUG_LOOKUP = {
    "Rates by Race": "rates",
    "Number of Stops by Race": "stops",
    "Search statistics": "search",
}

RACE_COLUMNS = [
    "Total",
    "White",
    "Black",
    "Hispanic",
    "Native American",
    "Asian",
    "Other",
]

def _normalize_text(text: str) -> str:
    """Strip ligatures & smart quotes to plain ASCII."""
    text = unicodedata.normalize("NFKC", text)
    return text.replace("ﬀ", "ff").replace("’", "'").replace(".", "")


# Accept numbers like 10, 10.5, 1,234.56, and leading-dot decimals like .27, with optional trailing %
_NUM_RE = re.compile(r"^(?:\d[\d,]*\.?\d*|\.\d+)%?$")
_DOTS_RE = re.compile(r"^[\.·]{2,}$")
_NUM_TOKEN_RE = re.compile(r"^(?:\d+\.\d+|\d+|\.\d+|\.)$")


def _is_numeric(tok: str) -> bool:
    tok = tok.strip()
    # Treat only real numbers (with optional commas/decimals) as numeric.
    # Dot leaders like "." are NOT considered numeric to avoid false positives.
    return bool(_NUM_RE.match(tok))


def _clean_numeric_str(tok: str) -> str:
    if tok is None or (isinstance(tok, float) and pd.isna(tok)):
        return ""
    t = str(tok).strip().replace(",", "")
    return t[:-1] if t.endswith("%") else t


def _slugify_simple(value: str) -> str:
    text = value.lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-{2,}", "-", text)
    return text.strip("-")


def _slugify_metric(value: str) -> str:
    return _slugify_simple(value.replace("%", "pct"))


def _build_metric_slug(table_slug: str, section: str | None, metric: str) -> str:
    metric_part = _slugify_metric(metric)
    if section:
        section_part = _slugify_metric(section)
        return f"{table_slug}--{section_part}--{metric_part}"
    return f"{table_slug}-{metric_part}"


def _build_metric_id(table_id: str, section: str | None, metric: str) -> str:
    parts = [table_id]
    if section:
        parts.append(section)
    parts.append(metric)
    return _slugify_simple("-".join(parts))


def _build_section_id(section: str | None) -> str | None:
    if not section:
        return None
    return _slugify_metric(section)


def _build_row_id(year: int | None, agency: str, table_id: str, section: str | None, metric: str) -> str:
    parts = [str(year)] if year is not None else []
    parts.append(_slugify_simple(agency))
    parts.append(_slugify_simple(table_id))
    if section:
        parts.append(_slugify_metric(section))
    parts.append(_slugify_metric(metric))
    return "-".join(parts)


def _normalize_line(line: str) -> str:
    text = unicodedata.normalize("NFKC", line)
    text = text.replace("’", "'").replace("“", '"').replace("”", '"')
    return text.replace("\x0c", "").strip()


def _clean_agency_name(name: str) -> str:
    text = _normalize_text(name)
    text = text.replace(" 's", "'s").strip()
    text = re.sub(r"\s+", " ", text)
    return text


def _is_page_number(line: str) -> bool:
    stripped = line.strip()
    return stripped.isdigit()


def _is_race_header(line: str) -> bool:
    tokens = [t.strip() for t in line.split() if t.strip()]
    target = ["total", "white", "black", "hispanic", "native", "american", "asian", "other"]
    idx = 0
    for tok in tokens:
        if idx < len(target) and tok.lower() == target[idx]:
            idx += 1
            if idx == len(target):
                return True
    return False


def _normalize_numeric_token(tok: str, *, allow_dot: bool) -> str | None:
    cleaned = tok.strip().replace(",", "")
    if cleaned.endswith("%"):
        cleaned = cleaned[:-1]
    if not allow_dot and cleaned == ".":
        return None
    return cleaned if _NUM_TOKEN_RE.match(cleaned) else None


def _parse_section_header(line: str) -> str | None:
    tokens = [t for t in line.split() if t]
    if not tokens:
        return None
    if any(_normalize_numeric_token(tok, allow_dot=False) for tok in tokens):
        return None
    if not any(_DOTS_RE.match(tok) or tok == "." for tok in tokens):
        return None
    label_tokens = [tok for tok in tokens if not (_DOTS_RE.match(tok) or tok == ".")]
    label = " ".join(label_tokens).strip()
    return label or None


def _parse_metric_line(line: str) -> tuple[str, list[str | None]] | None:
    tokens = [t for t in line.split() if t]
    if not tokens:
        return None

    numeric_values: list[str | None] = []
    first_numeric_idx: int | None = None
    for idx in range(len(tokens) - 1, -1, -1):
        token = tokens[idx]
        cleaned = _normalize_numeric_token(token, allow_dot=True)
        if cleaned is None:
            continue
        numeric_values.append(None if cleaned == "." else cleaned)
        first_numeric_idx = idx
        if len(numeric_values) == 7:
            break

    if len(numeric_values) < 7 or first_numeric_idx is None:
        return None

    numeric_values = list(reversed(numeric_values))
    label_tokens = [
        tok for tok in tokens[:first_numeric_idx] if not (_DOTS_RE.match(tok) or tok == ".")
    ]
    label = " ".join(label_tokens).strip()
    if not label:
        return None
    return label, numeric_values


def _should_end_table(line: str) -> bool:
    lowered = line.lower()
    return lowered.startswith("notes:") or lowered.startswith("agency notes:") or lowered.startswith("figure")


def _match_column_header(line: str) -> str | None:
    normalized = re.sub(r"\s+", " ", line).strip().lower()
    for col in RACE_COLUMNS:
        if normalized == col.lower():
            return col
    return None


_KNOWN_SECTIONS = {
    "population",
    "totals",
    "rates",
    "reason for stop",
    "stop outcome",
    "citation/warning violation",
    "arrest violation",
    "officer assignment",
    "location of stop",
    "driver gender",
    "driver age",
    "stops",
    "disparity index",
}


def _parse_plain_section_header(line: str) -> str | None:
    if any(_normalize_numeric_token(tok, allow_dot=False) for tok in line.split()):
        return None
    normalized = re.sub(r"\s+", " ", line).strip()
    if not normalized:
        return None
    if normalized.lower() in _KNOWN_SECTIONS:
        return normalized
    return None


def _ensure_pdftotext(pdf_path: Path, log) -> Path | None:
    txt_path = pdf_path.with_suffix(".layout.txt")
    try:
        if txt_path.exists() and txt_path.stat().st_mtime >= pdf_path.stat().st_mtime:
            log.debug("Using cached pdftotext output → %s", txt_path)
            return txt_path
    except Exception:
        pass

    cmd = ["pdftotext", "-layout", "-enc", "UTF-8", str(pdf_path), str(txt_path)]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        if result.stderr:
            log.debug("pdftotext stderr: %s", result.stderr.strip())
        return txt_path
    except FileNotFoundError:
        log.error("pdftotext not found on PATH; install poppler-utils to enable text parsing.")
    except subprocess.CalledProcessError as exc:
        log.error("pdftotext failed: %s", exc.stderr.strip() if exc.stderr else exc)
    return None


def _parse_pdftotext_lines(lines: list[str], log, *, year: int | None, pdf_name: str) -> pd.DataFrame:
    agency_re = re.compile(r"^\d+\.\d+\s+(.*\S)\s*$")
    table_re = re.compile(r"^(Table\s+\d+:\s+.+?)\s+for\s+(.+?)\s*$")

    armed = False
    current_agency: str | None = None
    current_table_title: str | None = None
    current_table_id: str | None = None
    current_table_slug: str | None = None
    current_section: str | None = None
    current_columns: list[str] | None = None
    pending_table_line: str | None = None
    column_mode = False
    column_row_labels: list[tuple[str, str | None]] = []
    column_data: dict[str, list[str | None]] = {}
    column_current: str | None = None

    records: list[dict] = []
    unparsed_counts: dict[tuple[str | None, str | None], int] = {}
    record_counts: dict[tuple[str | None, str | None], int] = {}
    stats = {
        "total_lines": 0,
        "blank_lines": 0,
        "page_number_lines": 0,
        "front_matter_lines": 0,
        "armed_lines": 0,
        "agency_headers": 0,
        "table_headers": 0,
        "section_headers": 0,
        "metric_rows": 0,
        "unparsed_lines": 0,
        "notes_lines": 0,
        "figure_lines": 0,
    }

    def _flush_columnar_records() -> None:
        nonlocal column_mode, column_row_labels, column_data, column_current
        if not column_row_labels or not column_data:
            column_mode = False
            column_row_labels = []
            column_data = {}
            column_current = None
            return
        if not current_table_id or not current_table_slug or not current_agency:
            column_mode = False
            column_row_labels = []
            column_data = {}
            column_current = None
            return
        for idx, (metric_label, section_label) in enumerate(column_row_labels):
            values = []
            for col in RACE_COLUMNS:
                col_values = column_data.get(col, [])
                if idx < len(col_values):
                    values.append(col_values[idx])
                else:
                    values.append(None)
            record = {
                "Total": values[0],
                "White": values[1],
                "Black": values[2],
                "Hispanic": values[3],
                "Native American": values[4],
                "Asian": values[5],
                "Other": values[6],
                "year": year,
                "slug": _build_metric_slug(current_table_slug, section_label, metric_label),
                "agency": current_agency,
                "table": current_table_title,
                "table_id": current_table_id,
                "section": section_label,
                "section_id": _build_section_id(section_label),
                "metric": metric_label,
                "metric_id": _build_metric_id(current_table_id, section_label, metric_label),
                "row_id": _build_row_id(year, current_agency, current_table_id, section_label, metric_label),
            }
            records.append(record)
            key = (current_agency, current_table_id)
            record_counts[key] = record_counts.get(key, 0) + 1
            stats["metric_rows"] += 1
        column_mode = False
        column_row_labels = []
        column_data = {}
        column_current = None

    for raw in lines:
        stats["total_lines"] += 1
        line = _normalize_line(raw)
        if not line:
            stats["blank_lines"] += 1
            continue
        if _is_page_number(line) and current_table_title is None and not column_mode:
            stats["page_number_lines"] += 1
            continue

        if not armed:
            if "agency results" in line.lower():
                armed = True
                log.info("Parser armed at Agency Results (%s)", pdf_name)
                continue
            agency_match = agency_re.match(line)
            if agency_match:
                armed = True
                current_agency = _clean_agency_name(agency_match.group(1))
                stats["agency_headers"] += 1
                log.info("Parser armed at agency header (%s): %s", pdf_name, current_agency)
                current_table_title = None
                current_table_id = None
                current_table_slug = None
                current_section = None
                current_columns = None
                continue
            stats["front_matter_lines"] += 1
            continue
        else:
            stats["armed_lines"] += 1

        agency_match = agency_re.match(line)
        if agency_match:
            _flush_columnar_records()
            current_agency = _clean_agency_name(agency_match.group(1))
            stats["agency_headers"] += 1
            current_table_title = None
            current_table_id = None
            current_table_slug = None
            current_section = None
            current_columns = None
            pending_table_line = None
            continue

        table_line = line
        if pending_table_line:
            table_line = f"{pending_table_line} {line}".strip()
        table_match = table_re.match(table_line)
        if table_match:
            _flush_columnar_records()
            full_title = table_match.group(1)
            raw_title = re.sub(r"^Table\s+\d+:\s*", "", full_title).strip()
            raw_agency = _clean_agency_name(table_match.group(2))
            current_table_title = raw_title
            current_table_id = _slugify_simple(raw_title)
            current_table_slug = TABLE_SLUG_LOOKUP.get(raw_title, current_table_id)
            current_section = None
            current_columns = None
            pending_table_line = None
            stats["table_headers"] += 1

            if current_agency and raw_agency and raw_agency != current_agency:
                log.warning(
                    "Table agency mismatch (%s): header=%s table=%s",
                    pdf_name,
                    current_agency,
                    raw_agency,
                )
            if not current_agency and raw_agency:
                current_agency = raw_agency
            continue
        if line.startswith("Table ") and " for " not in line:
            pending_table_line = line
            continue
        pending_table_line = None

        if not current_table_title:
            continue

        if _should_end_table(line):
            _flush_columnar_records()
            if line.lower().startswith("figure"):
                stats["figure_lines"] += 1
            else:
                stats["notes_lines"] += 1
            current_table_title = None
            current_table_id = None
            current_table_slug = None
            current_section = None
            current_columns = None
            continue

        if _is_race_header(line):
            current_columns = RACE_COLUMNS
            continue

        column_header = _match_column_header(line)
        if column_header and column_row_labels:
            column_mode = True
            column_current = column_header
            column_data.setdefault(column_header, [])
            continue

        if column_mode:
            column_header = _match_column_header(line)
            if column_header:
                column_current = column_header
                column_data.setdefault(column_header, [])
                continue
            if column_current:
                token = None
                for raw_token in line.split():
                    cleaned = _normalize_numeric_token(raw_token, allow_dot=True)
                    if cleaned is not None:
                        token = None if cleaned == "." else cleaned
                        break
                if token is None and line.strip() == ".":
                    token = None
                if token is not None or line.strip() == ".":
                    column_data[column_current].append(token)
                    continue

        section_label = _parse_section_header(line) or _parse_plain_section_header(line)
        if section_label:
            current_section = section_label
            stats["section_headers"] += 1
            continue

        if current_columns is None and not column_mode:
            column_row_labels.append((line, current_section))
            continue

        parsed = _parse_metric_line(line)
        if parsed and current_table_id and current_table_slug and current_agency:
            metric_label, values = parsed
            record = {
                "Total": values[0],
                "White": values[1],
                "Black": values[2],
                "Hispanic": values[3],
                "Native American": values[4],
                "Asian": values[5],
                "Other": values[6],
                "year": year,
                "slug": _build_metric_slug(current_table_slug, current_section, metric_label),
                "agency": current_agency,
                "table": current_table_title,
                "table_id": current_table_id,
                "section": current_section,
                "section_id": _build_section_id(current_section),
                "metric": metric_label,
                "metric_id": _build_metric_id(current_table_id, current_section, metric_label),
                "row_id": _build_row_id(year, current_agency, current_table_id, current_section, metric_label),
            }
            records.append(record)
            key = (current_agency, current_table_id)
            record_counts[key] = record_counts.get(key, 0) + 1
            stats["metric_rows"] += 1
            continue

        warn_key = (current_agency, current_table_id)
        count = unparsed_counts.get(warn_key, 0)
        if count < 5:
            log.warning(
                "Unparsed line (%s, %s, %s): %s",
                pdf_name,
                current_agency,
                current_table_id,
                line,
            )
        unparsed_counts[warn_key] = count + 1
        stats["unparsed_lines"] += 1

    _flush_columnar_records()

    if record_counts:
        sample = sorted(record_counts.items(), key=lambda item: item[1], reverse=True)[:5]
        log.info("Parsed records by agency/table (top 5): %s", sample)
    if unparsed_counts:
        worst = sorted(unparsed_counts.items(), key=lambda item: item[1], reverse=True)[:5]
        log.warning("Unparsed line counts (top 5): %s", worst)
    log.info(
        "Parser summary (%s): %s",
        pdf_name,
        {k: v for k, v in stats.items() if v},
    )

    df = pd.DataFrame(records)
    if df.empty:
        return pd.DataFrame(columns=EXTRACT_COLUMNS)

    df = df.reindex(columns=EXTRACT_COLUMNS)
    for col in NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col].map(_clean_numeric_str), errors="coerce")
    return df


def _add_extract_metadata(context, df: pd.DataFrame, label: str) -> None:
    if df is None:
        return
    meta = {
        "row_count": int(len(df)),
    }
    if not df.empty:
        if "agency" in df.columns:
            meta["unique_agencies"] = int(df["agency"].nunique(dropna=True))
        if "slug" in df.columns:
            meta["unique_slugs"] = int(df["slug"].nunique(dropna=True))
    try:
        context.add_output_metadata(meta)
        context.log.info("%s metadata: %s", label, meta)
    except Exception:
        pass

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
    """Yield a single page-range string for full-document text parsing."""
    total_pages = len(PdfReader(pdf_path).pages)
    page_range = f"1-{total_pages}"
    yield DynamicOutput(page_range, mapping_key="all_pages")


@op(out=Out(pd.DataFrame))
def parse_page_range(context, pdf_path: str, page_range: str) -> pd.DataFrame:
    """Extract every metric row from the PDF using pdftotext -layout."""
    del page_range
    pdf_file = Path(pdf_path)
    text_path = _ensure_pdftotext(pdf_file, context.log)
    if not text_path:
        return pd.DataFrame()

    try:
        text = text_path.read_text(encoding="utf-8", errors="ignore")
    except Exception as exc:
        context.log.error("Failed reading pdftotext output %s: %s", text_path, exc)
        return pd.DataFrame()

    year_match = re.search(r"(\d{4})", pdf_file.name)
    year = int(year_match.group(1)) if year_match else None
    lines = text.splitlines()
    df = _parse_pdftotext_lines(lines, context.log, year=year, pdf_name=pdf_file.name)
    _add_extract_metadata(context, df, f"extract_page_range_{pdf_file.name}")
    return df


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

    _add_extract_metadata(context, combined, f"extract_parquet_{year}")

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
