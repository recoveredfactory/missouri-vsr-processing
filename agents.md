# Agent Guide

Quick reference for future coding agents working on this Dagster pipeline that parses Missouri Vehicle Stops Report (VSR) PDFs into tidy data.

## What this project does
- Downloads per-year VSR PDFs (see `YEAR_URLS` in `missouri_vsr/assets/extract.py`) and stores them under `data/src/reports`.
- Extracts tables via `pdftotext -layout` with a text-first parser (`extract_pdf_data_<year>` assets), normalizing rows into `agency`, `table`, `section`, `metric`, and race counts/rates plus `row_key`/`row_id`.
- Combines per-year Parquet outputs (`data/processed/combined_output_<year>.parquet`) into a master Parquet + DataFrame (`combine_all_reports`), then pivots by row_key and emits per-agency JSON under `data/out/agency_year`. Also writes `data/out/report_dimensions.json` with unique table/section/metric ids.
- `data/src/2025-05-05-post-law-enforcement-agencies-list.xlsx` is agency metadata to join via a crosswalk; the `agency_list` asset loads it and writes `data/processed/agency_list.parquet` for downstream joins.

## Repo layout (essentials)
- `missouri_vsr/assets/`: Asset modules (`extract.py`, `reports.py`, `processed.py`, `audit.py`, `agency_reference.py`).
- `missouri_vsr/assets/extract.py`: PDF parsing logic (pdftotext layout parsing, section detection, metric identifiers).
- `missouri_vsr/definitions/definitions.py`: Dagster `Definitions`; registers assets and resources.
- `missouri_vsr/resources/resources.py`: S3, Airtable, Google Drive resources.
- `run_configs/*.yaml`: Example Dagster run configs (e.g., S3 bucket/prefix, WSL low-memory).
- `data/`: Local inputs/outputs (reports, processed parquet, JSON exports).

## Setup and execution
- Python 3.12+, `uv` for env mgmt (`uv sync`).
- Create `.env` (loaded by Dagster) for credentials:
  - AWS: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION` (or `AWS_REGION`), plus `MISSOURI_VSR_BUCKET_NAME`, `MISSOURI_VSR_S3_PREFIX` for S3 uploads.
  - Optional: `DAGSTER_HOME` to persist Dagster instance locally.
  - Optional resources: Airtable and Google service account vars if used.
- Run Dagster UI: `uv run dagster dev`.
- CLI materialization: `uv run dagster asset materialize --select <asset_or_query> -m missouri_vsr.definitions`.

## Operational notes
- `pdftotext -layout` outputs are cached next to the PDFs as `*.layout.txt`; delete those files to force re-extraction.
- Outputs may upload to S3 if the `s3` resource is configured or env vars are present; presigned URLs default to 45 days (`presigned_expiration`).
- Uses `row_key` for pivoting; row_keys encode `table_id` + `section_id` + `metric_id` (e.g., `number-of-stops-by-race--citation-warning-violation--moving`). `row_id` is `<year>-<agency_slug>-<row_key>`. Per-agency JSON is row-based (flat rows with row_key + identifiers).
- Keep an eye on PDF parsing heuristics (section detection, right-to-left numeric sniffing); adjust in `missouri_vsr/assets/extract.py` if tables shift.
- Data directories are configured via resources: `data_dir_source`, `data_dir_report_pdfs`, `data_dir_processed`, `data_dir_out` (see `definitions.py`).

## Data checks (csv-driven queue)
- `data_checks/row_sanity_checks.csv` drives per-year aggregate checks against `combine_all_reports`, matching on `row_key`, `agency`, and `year`, then asserting provided numeric/metadata fields. Each check returns counts plus samples of missing/mismatched rows.
- `checked` is for human review tracking only; checks still run for all rows.
- Add more checks by appending rows to that CSV; columns are coerced to numbers where applicable.
- Other checks in `asset_checks.py`: schema columns match, no duplicate `agency`+`row_key`+`year`, no duplicate `row_id`+`year`, and numeric columns parse.

## Dev runs
- Default dev workflow typically materializes everything, but you can select subsets in Dagster (e.g., a single `download_reports` output or one `extract_pdf_data_<year>` asset) and tune `VSR_PAGE_CHUNK_SIZE` to process smaller page chunks.
- `download_reports` honors `context.selected_output_names` to choose specific years even though `YEAR_URLS` is hard-coded; use asset selection in Dagster UI/CLI to leverage this.
- You can override PDF inputs by pointing the `data_dir_report_pdfs` resource at a directory containing example PDFs (e.g., a trimmed 2023 sample) instead of downloading from `YEAR_URLS`.
- Bundled sample: `data/src/examples/VSRreport2023.pdf` (pages 1694–1745). Run with `run_configs/example_2023_sample.yaml` and `--select extract_pdf_data_2023` to stay on the sample.

## Agency crosswalk CLI
- Script: `python -m missouri_vsr.cli.crosswalk` (run via `uv run …`).
- Defaults: reads agency metadata from `data/processed/agency_list.parquet` (or Excel fallback), VSR candidates from `data/processed/all_combined_output.parquet`, writes `data/src/agency_crosswalk.csv`, and optional merged join to `data/processed/agency_reference.parquet`.
- Auto-picks a name column unless `--name-col` is set; uses fuzzy suggestions from VSR “agency” values. Auto-accepts exact normalized matches.
- Interactive controls: pick a suggestion; `n` mark “not in VSR” (blank canonical); `s` skip for later (leave unresolved); `m` more; `b` back; `q` save/quit. Progress autosaves crosswalk + `.state.json` (same dir) for resume.
- Crosswalk is still evolving; current spreadsheet columns are in flux—don’t overfit.

## Pre-2020 vs 2020+ format split
- Years < 2020 use a completely different PDF layout and require `_parse_pre2020_pdftotext_lines()` in `extract.py`.
- Gate is in `parse_page_range`: dispatch by `year < 2020`.
- Pre-2020 race column order: `Total White Black Hispanic Asian Am. Indian Other` (Asian before Am. Indian — opposite of 2020+).
- Pre-2020 tables: `KEY INDICATORS`, `VEHICLE STOP STATS`, `SEARCH STATS` (not "Table N: ... for ...").
- Pre-2020 section labels are inline/multi-line (e.g., "Reason" on same line as first metric, "for stop" on next line alone).
- Pre-2020 null tokens: `N/A` and `#Num!` are valid null placeholders in numeric columns — do NOT drop those rows.
- Pre-2020 end marker: `Agency response` or `Notes:` (not `Agency notes:`).

## Extract philosophy
- **Extract close to source.** Per-year extract outputs should mirror the PDF as closely as possible. Do NOT normalize metric labels, section names, race column names, or spelling variants (e.g., keep "Am. Indian", "equpiment", "parol officer" as-is).
- Normalization and crosswalk mapping belong in `combine_all_reports` or downstream steps only.
- If a metric appears in some years but not others, that's fine — capture everything the PDF contains.

## Open questions for the human
- Should we document or check in sample/example PDFs for quick regression runs, or always hit the live AGO URLs?
- Is there a preferred minimal asset selection to run during development (e.g., a single year) and a standard run config we should default to in commands?
- Any additional external resources (Airtable/Drive) we expect to wire in soon that should be captured here?
