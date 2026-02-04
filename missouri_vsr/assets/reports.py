from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import requests

from dagster import AssetIn, AssetKey, AssetOut, In, Out, graph_asset, op, multi_asset

from missouri_vsr.assets.extract import YEAR_URLS, _ensure_pdftotext
from missouri_vsr.assets.s3_utils import upload_file_to_s3

AGENCY_RESPONSE_URLS = {
    2024: "https://ago.mo.gov/wp-content/uploads/2024-Agency-Responses-1.pdf",
    2023: "https://ago.mo.gov/wp-content/uploads/VSRagencynotes2023.pdf",
    2022: "https://ago.mo.gov/wp-content/uploads/2022-agency-comments-ago.pdf",
    2021: "https://ago.mo.gov/wp-content/uploads/2021-VSR-Agency-Comments.pdf",
    2020: "https://ago.mo.gov/wp-content/uploads/2020-VSR-Agency-Comments.pdf",
}

_COMMENT_MARKER_RE = re.compile(r"agency public comments?|public agency comments", re.IGNORECASE)


def _normalize_agency_name(value: str) -> str:
    text = (
        value.replace("’", "'")
        .replace("‘", "'")
        .replace("`", "'")
    )
    text = re.sub(r"\s+'s\b", "'s", text)
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()


def _is_comment_marker(line: str) -> bool:
    return bool(_COMMENT_MARKER_RE.search(line))


def _parse_agency_header(line: str) -> str | None:
    text = _normalize_agency_name(line)
    if not text:
        return None
    lower = text.lower()
    if "contents" == lower or "table of contents" in lower:
        return None
    if "agency comments" == lower or "agency responses" == lower:
        return None
    if _is_comment_marker(text):
        return None
    tokens = text.split()
    if not tokens:
        return None
    idx = 0
    while idx < len(tokens) and re.fullmatch(r"\d+[.)]?", tokens[idx]):
        idx += 1
    if idx == 0:
        return None
    tokens = tokens[idx:]
    if tokens and re.fullmatch(r"\d+", tokens[-1]):
        tokens = tokens[:-1]
    if not tokens:
        return None
    if any(re.fullmatch(r"\d+", tok) for tok in tokens):
        return None
    name = " ".join(tokens).strip()
    if not re.search(r"[A-Za-z]", name):
        return None
    return _normalize_agency_name(name)


def _collapse_comment_lines(lines: list[str]) -> str:
    parts: list[str] = []
    for line in lines:
        text = line.strip()
        if not text:
            if parts and parts[-1] != "":
                parts.append("")
            continue
        parts.append(text)
    if not parts:
        return ""
    text = "\n".join(parts).strip()
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


def _parse_agency_comments(lines: list[str], *, year: int, pdf_name: str, url: str, log) -> list[dict]:
    normalized_lines = []
    for raw in lines:
        cleaned = raw.replace("\x0c", "")
        cleaned = _normalize_agency_name(cleaned)
        normalized_lines.append(cleaned)

    marker_mode = any(_is_comment_marker(line) for line in normalized_lines)
    records: dict[str, dict] = {}

    current_agency: str | None = None
    current_lines: list[str] = []
    pending_agency: str | None = None
    pending_age = 0
    capturing = False

    def _commit():
        nonlocal current_agency, current_lines, capturing
        if not current_agency:
            return
        comment = _collapse_comment_lines(current_lines)
        record = {
            "year": year,
            "agency": current_agency,
            "comment": comment or None,
            "has_comment": bool(comment),
            "source_pdf": pdf_name,
            "source_url": url,
        }
        existing = records.get(current_agency)
        if existing is None or (record["has_comment"] and not existing["has_comment"]):
            records[current_agency] = record
        current_agency = None
        current_lines = []
        capturing = False

    for line in normalized_lines:
        if pending_agency:
            pending_age += 1
            if pending_age > 5:
                pending_agency = None
        if not line.strip():
            if capturing:
                current_lines.append("")
            continue
        if line.strip().isdigit():
            continue

        header = _parse_agency_header(line)
        if header:
            if marker_mode:
                if capturing:
                    _commit()
                pending_agency = header
                pending_age = 0
                continue
            if current_agency:
                _commit()
            current_agency = header
            current_lines = []
            capturing = True
            continue

        if marker_mode and _is_comment_marker(line):
            if pending_agency and pending_age <= 3:
                if current_agency:
                    _commit()
                current_agency = pending_agency
                current_lines = []
                capturing = True
            pending_agency = None
            continue

        if capturing and current_agency:
            current_lines.append(line)

    if current_agency:
        _commit()

    log.info(
        "Parsed %d agency comment rows from %s (%s)",
        len(records),
        pdf_name,
        year,
    )
    return list(records.values())

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
            "downloads/all_combined_output.parquet",
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


@multi_asset(
    outs={
        f"agency_response_{year}": AssetOut(metadata={"url": url})
        for year, url in AGENCY_RESPONSE_URLS.items()
    },
    group_name="responses",
    can_subset=True,
    required_resource_keys={"data_dir_source"},
    description="Download VSR agency response PDFs for each year.",
)
def download_agency_responses(context):
    selected = (
        set(context.selected_output_names)
        if context.selected_output_names
        else set(f"agency_response_{year}" for year in AGENCY_RESPONSE_URLS)
    )
    out_dir = Path(context.resources.data_dir_source.get_path()) / "agency_comments"
    out_dir.mkdir(parents=True, exist_ok=True)

    for year, url in AGENCY_RESPONSE_URLS.items():
        output_name = f"agency_response_{year}"
        if output_name not in selected:
            context.log.debug("Skipping %s (not selected)", output_name)
            continue
        pdf_path = out_dir / f"VSR_agency_comments_{year}.pdf"
        if not pdf_path.exists():
            context.log.info("Downloading agency responses %s → %s", url, pdf_path)
            resp = requests.get(url, timeout=(10, 300))
            resp.raise_for_status()
            pdf_path.write_bytes(resp.content)
        else:
            context.log.debug("Using cached %s", pdf_path)
        yield Output(
            str(pdf_path),
            output_name=output_name,
            metadata={"url": url, "local_path": str(pdf_path), "year": year},
        )


@op(
    out=Out(pd.DataFrame),
    ins={f"agency_response_{year}": In(str) for year in AGENCY_RESPONSE_URLS},
    required_resource_keys={"data_dir_processed"},
)
def parse_agency_comments(context, **response_paths: dict[str, str]) -> pd.DataFrame:
    records: list[dict] = []
    for year, url in AGENCY_RESPONSE_URLS.items():
        key = f"agency_response_{year}"
        path_str = response_paths.get(key)
        if not path_str:
            context.log.warning("Missing agency response path for %s", year)
            continue
        pdf_path = Path(path_str)
        text_path = _ensure_pdftotext(pdf_path, context.log)
        if text_path is None:
            context.log.error("Skipping agency comments for %s (pdftotext failed)", year)
            continue
        try:
            lines = text_path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except Exception as exc:
            context.log.error("Failed reading %s: %s", text_path, exc)
            continue
        records.extend(
            _parse_agency_comments(
                lines,
                year=year,
                pdf_name=pdf_path.name,
                url=url,
                log=context.log,
            )
        )

    comments = pd.DataFrame(records)
    processed_dir = Path(context.resources.data_dir_processed.get_path())
    out_path = processed_dir / "agency_comments.parquet"
    comments.to_parquet(out_path, index=False, engine="pyarrow")
    context.log.info("Wrote agency comments Parquet → %s (%d rows)", out_path, len(comments))

    try:
        meta = {"local_path": str(out_path), "row_count": len(comments)}
        if not comments.empty:
            meta["agency_count"] = int(comments["agency"].nunique(dropna=True))
            meta["year_count"] = int(comments["year"].nunique(dropna=True))
        context.add_output_metadata(meta)
    except Exception:
        pass
    return comments


@graph_asset(
    name="agency_comments",
    group_name="responses",
    ins={f"agency_response_{year}": AssetIn(key=AssetKey(f"agency_response_{year}")) for year in AGENCY_RESPONSE_URLS},
    description="Parse agency response PDFs into a per-agency comment table.",
)
def agency_comments(**response_paths: str) -> pd.DataFrame:
    return parse_agency_comments(**response_paths)
