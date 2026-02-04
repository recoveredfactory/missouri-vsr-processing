# Missouri Vehicle Stops Report Dagster Project

Process data from the [Missouri Vehicle Stops report](https://ago.mo.gov/get-help/vehicle-stops-report/) (VSR). 

This project is a data pipeline implemented with Dagster. It extracts data from PDFs published by the state (via `pdftotext -layout` and a text-first parser) to create a canonical database in a tidy format. It joins those records to additional information we have collected: a) A database of contact information and type for each agency, and b) Census data and geography obtained via geographic looks using [geocod.io](https://geocodio).

The rig outputs data for use in analysis and a web-based editorial product, including JSON data files and pmtiles maps for geospatial analysis and display.

In the future, we also plan to join the agency's comments on their VSR submission.

# Project Setup and Execution

## Prerequisites

Ensure you have the following installed:

- Python 3.12+ 
- [uv](https://docs.astral.sh/uv/) for dependency management (`brew install uv`)
- Poppler (`pdftotext` must be available on `PATH`; `brew install poppler` or `apt-get install poppler-utils`)

## Installation

Clone the repository and navigate into the project directory:

```sh
git clone https://github.com/themarshallproject/deaths-in-custody-processing
cd deaths-in-custody-processing
```

Set up the project with `uv`:

```sh
uv sync
```

## Credentials

### Environment variables 

The project relies on environment variables for Airtable, Google Drive, and S3 configurations. Create a `.env` file in the project root (it’s gitignored) with your values; Dagster loads it on startup. Typical entries:

```ini
AWS_ACCESS_KEY_ID=<your-access-key-id>
AWS_SECRET_ACCESS_KEY=<your-secret-key>
AWS_DEFAULT_REGION=us-east-1
MISSOURI_VSR_BUCKET_NAME=<shared-s3-bucket>
MISSOURI_VSR_S3_PREFIX=missouri-vsr/shared/
```

`AWS_*` are the standard boto3 variables. `MISSOURI_VSR_BUCKET_NAME` and `MISSOURI_VSR_S3_PREFIX` tell the Dagster S3 resource which bucket to write to and what “directory” (key prefix) to place outputs under; the resource will pick these up automatically from the environment. You can also load `run_configs/s3_shared_bucket.yaml` in the Dagster UI/CLI to apply the bucket/prefix when materializing assets.

If you see an error like “The authorization mechanism you have provided is not supported. Please use AWS4-HMAC-SHA256,” ensure `AWS_DEFAULT_REGION` (or `AWS_REGION`) is set to the bucket’s region; the S3 client is forced to use SigV4.

Presigned URLs default to 45 days; override via `presigned_expiration` in the S3 resource config (see `run_configs/s3_shared_bucket.yaml`).

If you work at The Marshall Project, these credentials can be found in the data team folder of TMP's 1password instance.

### Service account credentials

You must also have credentials for the project service account. To recreate for yourself, see the addendum in this resume. If you work for The Marshall Project, you can find this file in the data team folder of our 1password instance. Put it into `service_account.json` in the project's main directory or specify its location with the `GOOGLE_APPLICATION_CREDENTIALS` environment variable. 

## Running the Pipeline

You can either run the pipeline via the command line or a local web UI. 

To start the Dagster UI:

```sh
uv run dagster dev
```

This will provide an interface to run and monitor assets, typically running at http://localhost:3000.

The main entry point is the "asset catalog" (the "assets" tab in the UI) where you can "materialize" assets to create a local versions of the data for use downstream.

See `about-the-data.md` for a full narrative description of sources, formats, and metrics.

You can also materialize these assets via the command line:

```sh
uv run dagster asset materialize --select ASSET_NAME -m missouri_vsr.definitions
```

Dagster caches materialized assets, but they don't persist between runs of the web UI or CLI tool unless you set the `DAGSTER_HOME` environment variable in your global environment or `.env` file. See the [Dagster docs](https://docs.dagster.io/guides/deploy/dagster-instance-configuration#default-local-behavior) page on local behavior for more information.

### Text extraction cache

`pdftotext -layout` output is cached alongside each PDF as `*.layout.txt`. Delete those cached files to force a re-extraction.

### Key outputs

- Per-year extracts: `data/processed/combined_output_<year>.parquet`
- Combined extract: `data/processed/all_combined_output.parquet`
- Agency comments (parsed): `data/processed/agency_comments.parquet`
- Per-agency JSON (row-based): `data/out/agency_year/<agency_slug>.json`
- Metric-year JSON (per row_key): `data/out/metric_year/<row_key>.json`
- Metric-year subset (compact): `data/out/metric_year_subset.json`
- Report dimension index: `data/out/report_dimensions.json`
- Statewide baselines: `data/out/statewide_slug_baselines.json`
- Statewide per-year sums: `data/out/statewide_year_sums.json` (base rows exclude `-rate` and population row_keys; derived statewide rates are recomputed from totals + ACS population). Also includes `no-mshp--*` rows (sum across agencies excluding Missouri State Highway Patrol) and `avg-no-mshp--*` rows (mean across agencies excluding Missouri State Highway Patrol; derived rates computed per-agency then averaged).

Extracted rows include the identifiers:
- `table_id`: slug of the table title (no table number)
- `section_id`: slug of the section header
- `metric_id`: slug of the metric label
- `row_key`: `<table_id>--<section_id>--<metric_id>`
- `row_id`: `<year>-<agency_slug>-<row_key>`
- ACS population rows are normalized in `combine_all_reports` so any `*--population--YYYY-acs-pop`, `*--population--YYYY-pop`, and `*--population--YYYY-acs-pop-pct`/`YYYY-pop-pct` row_keys become `*--population--acs-pop` / `*--population--acs-pop-pct` for cross-year comparability (applies to `rates-by-race` and `disparity-index-by-race`).

The compact metric-year subset uses indexed rows to reduce size:

```json
{
  "agencies": ["Adair County Sheriff's Dept", "..."],
  "years": [2020, 2021, 2022, 2023, 2024],
  "columns": ["agency_idx", "year_idx", "Total", "White", "Black", "Hispanic", "Native American", "Asian", "Other"],
  "rows": {
    "rates-by-race--totals--all-stops": [
      [0, 3, 317, 281, 25, 8, 0, 2, 1]
    ]
  }
}
```

To decode each row: `agency = agencies[row[0]]`, `year = years[row[1]]`, and the remaining values align with `columns[2:]`.

### Quick sample run (2023 slice)

- A 50-page slice of the 2023 VSR (pages 1694–1745) lives at `data/src/examples/VSRreport2023.pdf`.
- Use `run_configs/example_2023_sample.yaml` to point `data_dir_report_pdfs` at that examples directory and keep outputs local.
- Materialize just the 2023 extract with:  
  `uv run dagster asset materialize --select extract_pdf_data_2023 -m missouri_vsr.definitions -c run_configs/example_2023_sample.yaml`
- `download_reports` will reuse the bundled PDF in `data/src/examples` and skip downloading.

### Marimo notebook (statewide sums)

After materializing `statewide_year_sums_json`, you can open the Marimo notebook at
`notebooks/marimo/statewide_year_sums.py` to view the statewide totals table.

## Agency crosswalk CLI

Interactive helper to map agency names from the metadata spreadsheet to canonical “agency” values seen in the VSR output.

Prereqs: have `data/processed/all_combined_output.parquet` (run `combine_all_reports`) and the agency metadata (`data/src/2025-05-05-post-law-enforcement-agencies-list.xlsx` or the Parquet it produces).

Run (defaults shown):

```sh
uv run python -m missouri_vsr.cli.crosswalk \
  --source-parquet data/processed/agency_list.parquet \
  --source-excel data/src/2025-05-05-post-law-enforcement-agencies-list.xlsx \
  --vsr-parquet data/processed/all_combined_output.parquet \
  --crosswalk data/src/agency_crosswalk.csv \
  --merge-output data/processed/agency_reference.parquet
```

Behavior highlights:
- Picks a name column automatically (agency/department/name) unless `--name-col` is provided.
- Suggests candidate agencies (from combined VSR output) using fuzzy matching; accept, mark “not in VSR” (`n`), skip for later (`s`), back, or page for more.
- Auto-fills exact normalized matches; writes progress on every decision to `agency_crosswalk.csv` and resume state to `agency_crosswalk.state.json` (same directory).
- Optional merged output (`agency_reference.parquet`) joins the agency metadata with the crosswalk for downstream joins.

## Geocodio debug CLI

Quickly geocode a single address and inspect the full Geocodio response. By default it requests the same fields used by the pipeline.

```sh
uv run vsr-parse geocode "705 East Walnut, Columbia, MO 65201"
```

To override fields:

```sh
uv run vsr-parse geocode "705 East Walnut, Columbia, MO 65201" --fields "cd,stateleg,acs-demographics"
```

Sample one agency per AgencyType (writes temp outputs under `data/processed/tmp_geocode`):

```sh
uv run vsr-parse geocode-sample
```
