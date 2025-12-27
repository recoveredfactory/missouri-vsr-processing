from __future__ import annotations

import argparse
import json
import logging
import os
import re
from pathlib import Path

import camelot
import httpx
import matplotlib.pyplot as plt
import pandas as pd
from dagster import build_op_context

from missouri_vsr.assets.agency_reference import GEOCODIO_FIELDS
from missouri_vsr.assets.agency_reference import build_agency_reference, geocode_agency_reference
from missouri_vsr.assets.extract import _clean_camelot_table
from missouri_vsr.resources import LocalDirectoryResource


def parse_pdf_to_df(
    pdf_path: str,
    pages: str | None = None,
    *,
    log: logging.Logger | None = None,
    plot_tables: bool = False,
) -> pd.DataFrame:
    log = log or logging.getLogger("vsr_cli")
    p = Path(pdf_path)
    if not p.exists():
        raise FileNotFoundError(p)

    # Camelot accepts '1-end' but we keep explicit pages if provided
    pages = pages or "1-end"

    try:
        tables = camelot.read_pdf(
            str(p),
            pages=pages,
            flavor="stream",
            edge_tol=50,
            row_tol=0,
            strip_text="\n",
        )
    except Exception as exc:
        raise RuntimeError(f"Camelot failed reading {p} pages={pages}: {exc}") from exc

    year_match = re.search(r"(\d{4})", p.name)
    year = int(year_match.group(1)) if year_match else None

    frames: list[pd.DataFrame] = []
    if plot_tables:
        dbg_dir = Path("debug") / p.stem
        dbg_dir.mkdir(parents=True, exist_ok=True)
        for idx, t in enumerate(tables):
            for kind in ["grid", "contour", "text"]:
                img_path = dbg_dir / f"{pages or 'all'}_tbl{idx}_{kind}.png"
                try:
                    ax = camelot.plot(t, kind=kind)
                    fig = ax.get_figure()
                    fig.savefig(img_path, dpi=200, bbox_inches="tight")
                    plt.close(fig)
                    log.info("Wrote debug image %s", img_path)
                except Exception as e:
                    log.warning("Plot failed for table %s kind %s: %s", idx, kind, e)

    for t in tables:
        cleaned = _clean_camelot_table(t, log, year=year)
        if cleaned is not None and not cleaned.empty:
            frames.append(cleaned)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _parse_fields(value: str | None) -> list[str] | None:
    if not value:
        return None
    fields = [item.strip() for item in value.split(",") if item.strip()]
    return fields or None


def _maybe_parse_json(value):
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except ValueError:
        return value


def geocode_address(
    address: str,
    *,
    api_key: str | None = None,
    fields: list[str] | None = None,
) -> dict | list:
    api_key = api_key or os.getenv("GEOCODIO_API_KEY")
    if not api_key:
        raise RuntimeError("GEOCODIO_API_KEY is not set.")
    field_list = fields or GEOCODIO_FIELDS
    params = {
        "q": address,
        "api_key": api_key,
    }
    if field_list:
        params["fields"] = ",".join(field_list)

    resp = httpx.get("https://api.geocod.io/v1.9/geocode", params=params, timeout=30.0)
    resp.raise_for_status()
    return resp.json()


def main(argv: list[str] | None = None) -> int:
    argv = list(argv) if argv is not None else None
    known_commands = {"parse", "geocode", "geocode-sample"}
    if argv and argv[0] not in known_commands:
        argv = ["parse", *argv]

    parser = argparse.ArgumentParser(description="Parse a VSR PDF or run geocoding debug.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    parse_parser = subparsers.add_parser("parse", help="Parse a VSR PDF and dump tidy data")
    parse_parser.add_argument("pdf", help="Path to PDF to parse")
    parse_parser.add_argument(
        "--pages",
        help="Camelot pages string (e.g., '1-5,7'). Defaults to all pages.",
        default=None,
    )
    parse_parser.add_argument(
        "--out",
        help="Output file path. If omitted, prints head() and row count.",
        default=None,
    )
    parse_parser.add_argument(
        "--format",
        choices=["json", "csv"],
        default="json",
        help="Output format when --out is provided (default: json)",
    )
    parse_parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    parse_parser.add_argument(
        "--plot-tables",
        action="store_true",
        help="Save Camelot debug plots for each table (grid/contour/text) to ./debug/",
    )

    geocode_parser = subparsers.add_parser("geocode", help="Geocode a single address")
    geocode_parser.add_argument("address", help="Address string to geocode")
    geocode_parser.add_argument(
        "--fields",
        help="Comma-separated Geocodio fields to request (defaults to pipeline fields).",
        default=None,
    )
    geocode_parser.add_argument(
        "--api-key",
        help="Override GEOCODIO_API_KEY",
        default=None,
    )
    geocode_parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    geocode_sample_parser = subparsers.add_parser(
        "geocode-sample",
        help="Geocode one agency per AgencyType and return JSON results",
    )
    geocode_sample_parser.add_argument(
        "--source-excel",
        default="data/src/2025-05-05-post-law-enforcement-agencies-list.xlsx",
        help="Path to the agency list Excel file.",
    )
    geocode_sample_parser.add_argument(
        "--data-dir-source",
        default="data/src",
        help="Directory containing agency_crosswalk.csv",
    )
    geocode_sample_parser.add_argument(
        "--data-dir-processed",
        default="data/processed/tmp_geocode",
        help="Directory for temporary geocode outputs.",
    )
    geocode_sample_parser.add_argument(
        "--per-type",
        type=int,
        default=1,
        help="Number of agencies to sample per AgencyType.",
    )
    geocode_sample_parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    log = logging.getLogger("vsr_cli")

    if args.command == "geocode":
        fields = _parse_fields(args.fields)
        response = geocode_address(args.address, api_key=args.api_key, fields=fields)
        print(json.dumps(response, indent=2, sort_keys=True, default=str))
        return 0

    if args.command == "geocode-sample":
        source_excel = Path(args.source_excel)
        if not source_excel.exists():
            raise FileNotFoundError(source_excel)

        df = pd.read_excel(source_excel, engine="openpyxl")
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].astype(str).str.strip()

        data_dir_processed = Path(args.data_dir_processed)
        data_dir_processed.mkdir(parents=True, exist_ok=True)
        resources = {
            "data_dir_source": LocalDirectoryResource(path=str(args.data_dir_source)),
            "data_dir_processed": LocalDirectoryResource(path=str(data_dir_processed)),
        }
        context = build_op_context(resources=resources)
        reference = build_agency_reference(context, df)
        sample = reference.groupby("AgencyType", dropna=False).head(args.per_type).copy()
        geocoded = geocode_agency_reference(context, sample)

        records = []
        for row in geocoded.to_dict(orient="records"):
            for key, value in row.items():
                if key.endswith("_response"):
                    row[key] = _maybe_parse_json(value)
            records.append(row)

        print(json.dumps(records, indent=2, sort_keys=True, default=str))
        return 0

    df = parse_pdf_to_df(args.pdf, pages=args.pages, log=log, plot_tables=args.plot_tables)
    if df.empty:
        log.warning("No tables extracted.")
        return 2

    if not args.out:
        # Print a small preview to stdout
        print(df.head(20).to_string())
        print(f"\nrows: {len(df)}")
        return 0

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if args.format == "json":
        df.to_json(out_path, orient="records", default_handler=str)
    else:
        df.to_csv(out_path, index=False)
    log.info("Wrote %d rows → %s", len(df), out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
