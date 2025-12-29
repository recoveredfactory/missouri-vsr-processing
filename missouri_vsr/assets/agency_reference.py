from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import List

import httpx
import pandas as pd
from slugify import slugify

from dagster import AssetIn, AssetKey, Out, graph_asset, op


def _env_csv(name: str, default: List[str]) -> List[str]:
    """Parse a comma-separated env var into a list."""
    raw = os.getenv(name)
    if not raw:
        return default
    items = [item.strip() for item in raw.split(",") if item.strip()]
    return items or default


GEOCODIO_FIELDS = _env_csv(
    "GEOCODIO_FIELDS",
    [
        "cd",
        "stateleg",
        "stateleg-next",
        "census",
        "acs-demographics",
        "acs-economics",
        "acs-social",
        "acs-housing",
        "acs-families",
    ],
)
GEOCODIO_FIELDS_MIN = _env_csv(
    "GEOCODIO_FIELDS_MIN",
    [
        "cd",
        "stateleg",
        "stateleg-next",
        "census"
    ],
)


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


def _env_float(name: str, default: float) -> float:
    """Parse a float from env var *name*, falling back to *default*."""
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


GEOCODIO_LIMIT = _env_positive_int("GEOCODIO_LIMIT", 5)
GEOCODIO_TIMEOUT = _env_float("GEOCODIO_TIMEOUT", 30.0)


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
# Agency reference (crosswalk applied)
# ------------------------------------------------------------------------------
@op(out=Out(pd.DataFrame), required_resource_keys={"data_dir_source", "data_dir_processed"})
def build_agency_reference(context, agency_list: pd.DataFrame) -> pd.DataFrame:
    """Join agency metadata to crosswalk and emit a canonical Department name."""
    from missouri_vsr.cli.crosswalk import _normalize_name

    crosswalk_path = Path(context.resources.data_dir_source.get_path()) / "agency_crosswalk.csv"
    if crosswalk_path.exists():
        crosswalk = pd.read_csv(crosswalk_path)
        context.log.info("Loaded crosswalk CSV → %s (%d rows)", crosswalk_path, len(crosswalk))
    else:
        context.log.warning("Crosswalk CSV not found at %s; output will have blank Canonical.", crosswalk_path)
        crosswalk = pd.DataFrame(columns=["Normalized", "Raw", "Canonical"])

    if agency_list.empty:
        context.log.warning("Agency list is empty; writing empty agency reference.")
        merged = agency_list.copy()
        merged["Normalized"] = pd.NA
        merged["Canonical"] = pd.NA
    else:
        name_col = next(
            (c for c in agency_list.columns if str(c).lower() in {"department", "agency", "name"}),
            agency_list.columns[0],
        )
        context.log.info("Using agency name column for crosswalk: %s", name_col)
        merged = agency_list.copy()
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
    ins={"agency_list": AssetIn(key=AssetKey("agency_list"))},
    description="Join agency list metadata to the crosswalk and output canonical Department names.",
)
def agency_reference_asset(agency_list: pd.DataFrame) -> pd.DataFrame:
    return build_agency_reference(agency_list)


# ------------------------------------------------------------------------------
# Agency geographies (raw Geocodio responses)
# ------------------------------------------------------------------------------

def _normalize_agency_type(value) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def _clean_city(value) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return ""
    return text


def _build_city_query(city: str) -> str:
    return f"{city}, MO" if city else ""


def _clean_address_part(value) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return ""
    return text


def _build_address_query(row: pd.Series) -> str:
    parts = []
    for key in ("AddressLine1", "AddressLine2", "AddressCity", "AddressZip"):
        part = _clean_address_part(row.get(key))
        if part:
            parts.append(part)
    if not parts:
        return ""
    has_state = any(re.search(r"\b(MO|Missouri)\b", part, flags=re.IGNORECASE) for part in parts)
    if not has_state:
        parts.append("MO")
    return ", ".join(parts)


def _result_has_county(result: dict) -> bool:
    components = result.get("address_components") if isinstance(result, dict) else None
    return bool(components and components.get("county"))


def _result_has_fields(result: dict) -> bool:
    fields = result.get("fields") if isinstance(result, dict) else None
    return bool(isinstance(fields, dict) and fields)


def _select_result_index(results: list, predicate) -> int | None:
    for idx, item in enumerate(results):
        if predicate(item):
            return idx
    return 0 if results else None


def _extract_county(result: dict) -> str:
    components = result.get("address_components") if isinstance(result, dict) else None
    if not components:
        return ""
    return str(components.get("county") or "").strip()


def _extract_county_from_results(results: list, selected_idx: int | None) -> str:
    if not results:
        return ""
    if selected_idx is not None and 0 <= selected_idx < len(results):
        county = _extract_county(results[selected_idx])
        if county:
            return county
    for item in results:
        county = _extract_county(item)
        if county:
            return county
    return ""


def _build_county_query(county_name: str) -> str:
    if not county_name:
        return ""
    county = county_name.strip()
    if not county:
        return ""
    if re.search(r"\bcounty\b", county, flags=re.IGNORECASE):
        return f"{county}, MO"
    return f"{county} County, MO"


def _dump_json(value) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=True)


def _geocode_request(
    client: httpx.Client,
    api_key: str,
    address: str,
    fields: List[str] | None,
    limit: int,
) -> tuple[dict | None, str | None]:
    params = {"q": address, "api_key": api_key}
    if fields:
        params["fields"] = ",".join(fields)
    if limit:
        params["limit"] = str(limit)
    try:
        resp = client.get("https://api.geocod.io/v1.9/geocode", params=params)
    except httpx.RequestError as exc:
        return None, str(exc)

    try:
        resp.raise_for_status()
    except httpx.HTTPStatusError:
        return None, f"HTTP {resp.status_code}: {resp.text}"

    try:
        return resp.json(), None
    except ValueError as exc:
        return None, f"Invalid JSON: {exc}"


@op(out=Out(pd.DataFrame), required_resource_keys={"data_dir_processed"})
def geocode_agency_reference(context, agency_reference: pd.DataFrame) -> pd.DataFrame:
    """Fetch raw Geocodio responses for agency geographies and store the JSON."""
    out_dir = Path(context.resources.data_dir_processed.get_path())
    out_path = out_dir / "agency_reference_geocoded.parquet"

    if agency_reference.empty:
        context.log.warning("Agency reference is empty; writing empty geocode output.")
        agency_reference.to_parquet(out_path, index=False, engine="pyarrow")
        return agency_reference

    api_key = os.getenv("GEOCODIO_API_KEY")
    working = agency_reference.copy()
    working["agency_slug"] = working["Department"].apply(
        lambda val: (
            slugify(str(val).replace("'", ""), lowercase=True)
            if pd.notna(val) and str(val).strip()
            else ""
        )
    )

    extra_cols = [
        "geocode_address_status",
        "geocode_address_reason",
        "geocode_address_query",
        "geocode_address_response",
        "geocode_address_selected_index",
        "geocode_address_error",
        "geocode_address_county",
        "geocode_jurisdiction_scope",
        "geocode_jurisdiction_status",
        "geocode_jurisdiction_reason",
        "geocode_jurisdiction_query",
        "geocode_jurisdiction_response",
        "geocode_jurisdiction_selected_index",
        "geocode_jurisdiction_error",
        "geocode_jurisdiction_county",
    ]
    for col in extra_cols:
        if col not in working.columns:
            working[col] = pd.NA

    if out_path.exists():
        try:
            existing = pd.read_parquet(out_path)
        except Exception as exc:
            context.log.warning("Failed reading existing geocode output %s: %s", out_path, exc)
            existing = pd.DataFrame()

        if not existing.empty and "Department" in existing.columns:
            existing = existing.drop_duplicates(subset=["Department"], keep="first")
            existing = existing.set_index("Department")
            for col in extra_cols:
                if col in existing.columns:
                    working[col] = working["Department"].map(existing[col])
            context.log.info("Loaded %d cached geocode rows from %s", len(existing), out_path)

    if not api_key:
        context.log.warning("GEOCODIO_API_KEY not set; writing output with status only.")
        working["geocode_address_status"] = "missing_api_key"
        working["geocode_jurisdiction_status"] = "missing_api_key"
        missing_count = len(working)
        context.log.info("Geocode summary: %d rows marked missing_api_key", missing_count)
        working.to_parquet(out_path, index=False, engine="pyarrow")
        return working

    address_status_counts: dict[str, int] = {}
    address_reason_counts: dict[str, int] = {}
    jurisdiction_status_counts: dict[str, int] = {}
    jurisdiction_reason_counts: dict[str, int] = {}

    def _track_status(prefix: str, idx, status: str, reason: str | None = None) -> None:
        working.loc[idx, f"{prefix}_status"] = status
        if reason:
            working.loc[idx, f"{prefix}_reason"] = reason
        if prefix == "geocode_address":
            address_status_counts[status] = address_status_counts.get(status, 0) + 1
            if reason:
                address_reason_counts[reason] = address_reason_counts.get(reason, 0) + 1
        else:
            jurisdiction_status_counts[status] = jurisdiction_status_counts.get(status, 0) + 1
            if reason:
                jurisdiction_reason_counts[reason] = jurisdiction_reason_counts.get(reason, 0) + 1

    def _set_response(prefix: str, idx, response: dict | None, selected_idx: int | None) -> None:
        working.loc[idx, f"{prefix}_response"] = _dump_json(response)
        working.loc[idx, f"{prefix}_selected_index"] = selected_idx
        results = response.get("results") if isinstance(response, dict) else []
        working.loc[idx, f"{prefix}_county"] = _extract_county_from_results(results, selected_idx)

    def _display_agency_name(row: pd.Series, fallback: str) -> str:
        for key in (
            "Department",
            "AgencyName",
            "Agency Name",
            "Agency",
            "DepartmentName",
            "Name",
        ):
            if key not in row:
                continue
            value = row.get(key)
            if pd.isna(value):
                continue
            text = str(value).strip()
            if text and text.lower() != "nan":
                return text
        return fallback

    def _classify_agency_type(value) -> str:
        text = _normalize_agency_type(value)
        if not text:
            return "address"
        if "county" in text:
            return "county"
        if "municipal" in text:
            return "municipal"
        if text == "state" or text == "state agency" or text.startswith("state "):
            return "state"
        return "address"

    state_cache: dict[str, object] = {"ready": False}
    address_batch_names: list[str] = []
    jurisdiction_batch_names: list[str] = []
    address_processed = 0
    jurisdiction_processed = 0
    total_rows = len(working)

    rows_by_scope = {
        "county": [],
        "municipal": [],
        "state": [],
        "address": [],
    }
    for idx, row in working.iterrows():
        scope = _classify_agency_type(row.get("AgencyType"))
        working.loc[idx, "geocode_jurisdiction_scope"] = scope
        rows_by_scope[scope].append(idx)

    def _note_address_progress(name: str) -> None:
        nonlocal address_processed, address_batch_names
        address_processed += 1
        address_batch_names.append(name)
        if address_processed % 25 == 0 or address_processed == total_rows:
            context.log.info("Address geocode progress: %d/%d", address_processed, total_rows)
            context.log.info("Address geocode batch agencies: %s", address_batch_names)
            address_batch_names = []

    def _note_jurisdiction_progress(name: str) -> None:
        nonlocal jurisdiction_processed, jurisdiction_batch_names
        jurisdiction_processed += 1
        jurisdiction_batch_names.append(name)
        if jurisdiction_processed % 25 == 0 or jurisdiction_processed == total_rows:
            context.log.info(
                "Jurisdiction geocode progress: %d/%d",
                jurisdiction_processed,
                total_rows,
            )
            context.log.info("Jurisdiction geocode batch agencies: %s", jurisdiction_batch_names)
            jurisdiction_batch_names = []

    with httpx.Client(timeout=GEOCODIO_TIMEOUT) as client:
        for idx, row in working.iterrows():
            agency_name = _display_agency_name(row, f"row:{idx}")
            status = row.get("geocode_address_status")
            if isinstance(status, str) and status in {"ok", "skipped"}:
                _note_address_progress(agency_name)
                continue
            query = _build_address_query(row)
            if not query:
                _track_status("geocode_address", idx, "skipped", "missing_address")
                _note_address_progress(agency_name)
                continue

            working.loc[idx, "geocode_address_query"] = query
            address_scope = working.loc[idx, "geocode_jurisdiction_scope"]
            address_fields = ["census"] if address_scope == "state" else GEOCODIO_FIELDS_MIN
            resp, err = _geocode_request(
                client,
                api_key,
                query,
                address_fields,
                GEOCODIO_LIMIT,
            )
            if err:
                working.loc[idx, "geocode_address_error"] = err
                _track_status("geocode_address", idx, "error", "address_geocode_failed")
                _note_address_progress(agency_name)
                continue

            results = resp.get("results") if isinstance(resp, dict) else []
            if not results:
                _track_status("geocode_address", idx, "error", "address_no_results")
                _note_address_progress(agency_name)
                continue

            selected_idx = _select_result_index(results, _result_has_fields)
            _set_response("geocode_address", idx, resp, selected_idx)
            _track_status("geocode_address", idx, "ok")
            _note_address_progress(agency_name)

        for idx in rows_by_scope["state"]:
            row = working.loc[idx]
            agency_name = _display_agency_name(row, f"row:{idx}")
            status = row.get("geocode_jurisdiction_status")
            if isinstance(status, str) and status in {"ok", "skipped"}:
                _note_jurisdiction_progress(agency_name)
                continue
            query = "Missouri"
            working.loc[idx, "geocode_jurisdiction_query"] = query
            if not state_cache["ready"]:
                resp, err = _geocode_request(
                    client,
                    api_key,
                    query,
                    ["census"],
                    GEOCODIO_LIMIT,
                )
                state_cache["ready"] = True
                state_cache["response"] = resp
                state_cache["error"] = err
                results = resp.get("results") if isinstance(resp, dict) else []
                state_cache["selected_idx"] = _select_result_index(results, _result_has_fields)

            if state_cache.get("error"):
                working.loc[idx, "geocode_jurisdiction_error"] = state_cache["error"]
                _track_status("geocode_jurisdiction", idx, "error", "state_geocode_failed")
            else:
                resp = state_cache.get("response")
                results = resp.get("results") if isinstance(resp, dict) else []
                if not results:
                    _track_status("geocode_jurisdiction", idx, "error", "state_no_results")
                else:
                    _set_response("geocode_jurisdiction", idx, resp, state_cache.get("selected_idx"))
                    _track_status("geocode_jurisdiction", idx, "ok")
            _note_jurisdiction_progress(agency_name)

        for idx in rows_by_scope["county"]:
            row = working.loc[idx]
            agency_name = _display_agency_name(row, f"row:{idx}")
            status = row.get("geocode_jurisdiction_status")
            if isinstance(status, str) and status in {"ok", "skipped"}:
                _note_jurisdiction_progress(agency_name)
                continue
            city = _clean_city(row.get("AddressCity"))
            if not city:
                _track_status("geocode_jurisdiction", idx, "skipped", "missing_city")
                _note_jurisdiction_progress(agency_name)
                continue

            city_query = _build_city_query(city)
            city_resp, city_err = _geocode_request(
                client,
                api_key,
                city_query,
                None,
                GEOCODIO_LIMIT,
            )
            if city_err:
                working.loc[idx, "geocode_jurisdiction_error"] = city_err
                _track_status("geocode_jurisdiction", idx, "error", "city_geocode_failed")
                _note_jurisdiction_progress(agency_name)
                continue

            results = city_resp.get("results") if isinstance(city_resp, dict) else []
            if not results:
                _track_status("geocode_jurisdiction", idx, "error", "city_no_results")
                _note_jurisdiction_progress(agency_name)
                continue

            selected_idx = _select_result_index(results, _result_has_county)
            if selected_idx is None:
                _track_status("geocode_jurisdiction", idx, "error", "county_not_found")
                _note_jurisdiction_progress(agency_name)
                continue

            county_name = _extract_county(results[selected_idx])
            if not county_name:
                _track_status("geocode_jurisdiction", idx, "error", "county_not_found")
                _note_jurisdiction_progress(agency_name)
                continue

            county_query = _build_county_query(county_name)
            if not county_query:
                _track_status("geocode_jurisdiction", idx, "error", "county_query_empty")
                _note_jurisdiction_progress(agency_name)
                continue

            working.loc[idx, "geocode_jurisdiction_query"] = county_query
            county_resp, county_err = _geocode_request(
                client,
                api_key,
                county_query,
                GEOCODIO_FIELDS,
                GEOCODIO_LIMIT,
            )
            if county_err:
                working.loc[idx, "geocode_jurisdiction_error"] = county_err
                _track_status("geocode_jurisdiction", idx, "error", "county_geocode_failed")
                _note_jurisdiction_progress(agency_name)
                continue

            county_results = county_resp.get("results") if isinstance(county_resp, dict) else []
            if not county_results:
                _track_status("geocode_jurisdiction", idx, "error", "county_no_results")
                _note_jurisdiction_progress(agency_name)
                continue

            county_selected = _select_result_index(county_results, _result_has_fields)
            _set_response("geocode_jurisdiction", idx, county_resp, county_selected)
            _track_status("geocode_jurisdiction", idx, "ok")
            _note_jurisdiction_progress(agency_name)

        for idx in rows_by_scope["municipal"]:
            row = working.loc[idx]
            agency_name = _display_agency_name(row, f"row:{idx}")
            status = row.get("geocode_jurisdiction_status")
            if isinstance(status, str) and status in {"ok", "skipped"}:
                _note_jurisdiction_progress(agency_name)
                continue
            city = _clean_city(row.get("AddressCity"))
            if not city:
                _track_status("geocode_jurisdiction", idx, "skipped", "missing_city")
                _note_jurisdiction_progress(agency_name)
                continue

            city_query = _build_city_query(city)
            working.loc[idx, "geocode_jurisdiction_query"] = city_query
            city_resp, city_err = _geocode_request(
                client,
                api_key,
                city_query,
                GEOCODIO_FIELDS,
                GEOCODIO_LIMIT,
            )
            if city_err:
                working.loc[idx, "geocode_jurisdiction_error"] = city_err
                _track_status("geocode_jurisdiction", idx, "error", "city_geocode_failed")
                _note_jurisdiction_progress(agency_name)
                continue

            results = city_resp.get("results") if isinstance(city_resp, dict) else []
            if not results:
                _track_status("geocode_jurisdiction", idx, "error", "city_no_results")
                _note_jurisdiction_progress(agency_name)
                continue

            selected_idx = _select_result_index(results, _result_has_fields)
            _set_response("geocode_jurisdiction", idx, city_resp, selected_idx)
            _track_status("geocode_jurisdiction", idx, "ok")
            _note_jurisdiction_progress(agency_name)

        for idx in rows_by_scope["address"]:
            row = working.loc[idx]
            agency_name = _display_agency_name(row, f"row:{idx}")
            status = row.get("geocode_jurisdiction_status")
            if isinstance(status, str) and status in {"ok", "skipped"}:
                _note_jurisdiction_progress(agency_name)
                continue
            _track_status("geocode_jurisdiction", idx, "skipped", "no_jurisdiction")
            _note_jurisdiction_progress(agency_name)

    address_status_counts = (
        working["geocode_address_status"].value_counts(dropna=True).to_dict()
        if "geocode_address_status" in working.columns
        else {}
    )
    address_reason_counts = (
        working["geocode_address_reason"].value_counts(dropna=True).to_dict()
        if "geocode_address_reason" in working.columns
        else {}
    )
    jurisdiction_status_counts = (
        working["geocode_jurisdiction_status"].value_counts(dropna=True).to_dict()
        if "geocode_jurisdiction_status" in working.columns
        else {}
    )
    jurisdiction_reason_counts = (
        working["geocode_jurisdiction_reason"].value_counts(dropna=True).to_dict()
        if "geocode_jurisdiction_reason" in working.columns
        else {}
    )

    working.to_parquet(out_path, index=False, engine="pyarrow")
    context.log.info("Wrote agency geocode Parquet → %s (%d rows)", out_path, len(working))
    context.log.info("Address geocode status counts: %s", address_status_counts)
    if address_reason_counts:
        top_reasons = sorted(address_reason_counts.items(), key=lambda x: x[1], reverse=True)[:8]
        context.log.info("Address geocode top reasons: %s", top_reasons)
    context.log.info("Jurisdiction geocode status counts: %s", jurisdiction_status_counts)
    if jurisdiction_reason_counts:
        top_reasons = sorted(jurisdiction_reason_counts.items(), key=lambda x: x[1], reverse=True)[:8]
        context.log.info("Jurisdiction geocode top reasons: %s", top_reasons)
    try:
        context.add_output_metadata({"local_path": str(out_path), "row_count": len(working)})
    except Exception:
        pass
    return working


@graph_asset(
    name="agency_reference_geocoded",
    group_name="processed",
    ins={"agency_reference": AssetIn(key=AssetKey("agency_reference"))},
    description="Fetch raw Geocodio responses for agency geographies.",
)
def agency_reference_geocoded_asset(agency_reference: pd.DataFrame) -> pd.DataFrame:
    return geocode_agency_reference(agency_reference)
