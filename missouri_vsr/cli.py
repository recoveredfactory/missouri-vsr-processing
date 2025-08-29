from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path

import camelot
import pandas as pd

from .assets import _clean_camelot_table


def parse_pdf_to_df(pdf_path: str, pages: str | None = None, *, log: logging.Logger | None = None) -> pd.DataFrame:
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
    for t in tables:
        cleaned = _clean_camelot_table(t, log, year=year)
        if cleaned is not None and not cleaned.empty:
            frames.append(cleaned)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Parse a VSR PDF and dump tidy data")
    parser.add_argument("pdf", help="Path to PDF to parse")
    parser.add_argument(
        "--pages",
        help="Camelot pages string (e.g., '1-5,7'). Defaults to all pages.",
        default=None,
    )
    parser.add_argument(
        "--out",
        help="Output file path. If omitted, prints head() and row count.",
        default=None,
    )
    parser.add_argument(
        "--format",
        choices=["json", "csv"],
        default="json",
        help="Output format when --out is provided (default: json)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    log = logging.getLogger("vsr_cli")

    df = parse_pdf_to_df(args.pdf, pages=args.pages, log=log)
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
