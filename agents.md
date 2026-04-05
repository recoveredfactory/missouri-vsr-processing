# Missouri VSR — Architecture & Agent Guide

## Overview

This project extracts, normalizes, and publishes Missouri's annual Vehicle Stops Report (VSR) — a state-mandated racial profiling dataset from the Missouri Attorney General's office — for use in an editorial web product at The Marshall Project.

The pipeline covers 2014–present across two distinct PDF formats (pre-2020 and 2020+), ~600 law enforcement agencies, and a growing span of years. Primary outputs are versioned, structured data files consumed by a separate frontend application.

**Design principles**

- *Extract close to source.* Raw parsing preserves the original report's structure, metric names, and values without interpretation. Normalization is a separate, explicit layer.
- *Reproducible by re-run.* Every output can be regenerated from source PDFs and config files in this repo.
- *Checks at the seam.* Parser regressions are caught at the per-year extract layer before downstream assets run.
- *Stable contracts.* The frontend pins to a versioned data release. Pipeline changes that affect outputs increment the release version rather than silently breaking consumers.
- *Agent-friendly.* The repo is structured so that AI coding agents working on the pipeline and on the frontend can operate independently against a shared data contract, minimizing coordination overhead.

**Tech stack**

- Dagster 1.x — orchestration (no schedules; batch on-demand)
- pandas / pyarrow — data wrangling
- pdftotext -layout (poppler-utils) — primary PDF extraction
- DuckDB — query layer for data exploration and (planned) API
- GeoPandas + tippecanoe — GIS / PMTiles
- S3 — versioned output distribution

**Repo layout (essentials)**

- `missouri_vsr/assets/` — Asset modules (`extract.py`, `reports.py`, `processed.py`, `audit.py`, `agency_reference.py`, `gis.py`)
- `missouri_vsr/asset_checks/` — Per-year and combined asset checks
- `missouri_vsr/definitions/` — Dagster `Definitions`; registers assets and resources
- `data_checks/` — CSV-driven row sanity checks per year
- `run_configs/` — Example Dagster run configs
- `data/` — Local inputs/outputs (reports, processed parquet, JSON exports)

**Setup**

- Python 3.12+; `uv sync` to install deps
- `uv run dagster dev` to start the Dagster UI
- Copy `.env.example` to `.env` and fill in AWS credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`, `MISSOURI_VSR_BUCKET_NAME`, `MISSOURI_VSR_S3_PREFIX`)

---

## Architecture

### Three-layer pipeline

Data moves through three conceptually distinct layers. Keeping them separate is what makes the system reproducible, testable, and safe to evolve.

```
Layer 1 — Raw / parsed
  Source PDFs → pdftotext → per-year DataFrames
  Assets: download_reports, extract_pdf_data_{year}
  Output: combined_output_{year}.parquet
  Row keys: era-specific (pre-2020 uses key-indicators/vehicle-stop-stats/search-stats;
            2020+ uses rates-by-race/number-of-stops-by-race/search-statistics)
  Invariant: no interpretation, no normalization; values match the source PDF exactly

Layer 2 — Normalized / processed
  Combined across years → canonical metrics → ranks, percentiles, baselines
  Assets: combine_all_reports, reports_with_rank_percentile,
          statewide_slug_baselines, agency_reference, gis
  Output: all_combined_output.parquet, reports_with_rank_percentile.parquet,
          per-agency JSON under data/out/agency_year/
  Row keys: both era-specific keys preserved AND a canonical_key column (planned)
            that maps equivalent metrics across eras for cross-year analysis
  Invariant: layer 1 outputs are never modified; normalization adds columns / rows

Layer 3 — Released
  Versioned snapshot promoted to S3 for frontend consumption
  Assets: dist group (JSON/GeoJSON/PMTiles exports, manifests)
  Output: releases/vN/ on S3 with a manifest.json describing schema version,
          years covered, canonical metric definitions
  Invariant: a released version is immutable; frontend pins to a version
```

**The era-coherence problem** (current active design question): Pre-2020 and 2020+ reports use different metric names for equivalent concepts. The planned solution is a `canonical_key` column populated at layer 2 from a crosswalk config — e.g., both `key-indicators--stops` (2014–2019) and `rates-by-race--totals--all-stops` (2020+) map to canonical key `stops`. Layer 1 is untouched; layer 2 gains cross-year query capability; layer 3 exposes only canonical metrics to the frontend.

### Staging + release workflow

```
main branch          → layer 1/2 assets; no versioned release
staging/vN/ on S3    → pipeline writes here; frontend smoke-tests against it
releases/vN/ on S3   → promoted from staging; frontend pins here
```

A release is triggered manually after:
1. All asset checks pass on main
2. Frontend team has validated the staging build
3. `manifest.json` is updated with schema version, year range, and changelog entry

Patch releases (vN.x) are backward-compatible changes (new years added, bug fixes that don't change existing values). Minor/major releases require a frontend coordination window and deprecate the prior release after a transition period.

### Testing strategy

Asset checks are the primary regression gate. They run at two levels:

**Per-year extract checks** (`extract_pdf_data_{year}`):
- `schema` — DataFrame columns match the expected set for that era
- `no_duplicate_row_keys` — no `(agency, row_key)` collisions within a year
- `numeric_columns_parse` — all race columns contain parseable numbers
- `row_expectations` — golden-file CSV (`data_checks/row_sanity_checks_{year}.csv`) with spot-checked agency/metric values verified against source PDFs

**Combined asset checks** (`combine_all_reports`):
- Schema, duplicate row_key/row_id checks across the full multi-year frame

**Adding sanity check rows**: pick an agency, read its PDF page, verify the values, append to `data_checks/row_sanity_checks_{year}.csv`. The `checked` column is for human review tracking; all rows run regardless.

Unit tests (`pytest`) cover normalization ops and processing helpers but not the PDF parser directly — parser regressions are caught via the golden-file checks above.

---

## Data + Release

### Release / version strategy

Data releases follow semantic versioning (vMAJOR.MINOR):

- **Major** — breaking schema change: columns renamed/removed, row_key structure changed, canonical metric definitions revised. Requires frontend coordination and a deprecation window for the prior major version.
- **Minor** — backward-compatible addition: new years added, new canonical metrics added, new derived columns added. Frontend can adopt at its own pace.

S3 layout:

```
s3://{bucket}/
  releases/
    v1/          ← current frozen release (2020–2024, era-specific row_keys)
    v2/          ← next release (2014–2024, canonical_key layer added)
  staging/
    v2/          ← pipeline writes here; not safe for production frontend use
  manifest.json  ← points to the current stable release version
```

Each release directory contains a `manifest.json`:

```json
{
  "version": "2.0",
  "released": "YYYY-MM-DD",
  "years": [2014, 2015, ..., 2024],
  "schema_version": "2.0",
  "canonical_metrics": ["stops", "searches", "arrests", ...],
  "changelog": "Added 2014–2019 pre-2020 format data with canonical_key normalization."
}
```

The frontend reads `manifest.json` to discover the current release and can hard-pin to a specific version for stability.

### Major migrations + milestones

**v1.0 — current** (2020–2024, 2020+ format only)
- Per-year extract assets with asset checks
- Era-specific row_keys (`rates-by-race--*`, `number-of-stops-by-race--*`, `search-statistics--*`)
- S3 flat-file outputs, per-agency JSON

**v2.0 — planned** (2014–2024, cross-era normalization)
- Pre-2020 data (2014–2019) added to pipeline
- `canonical_key` column added at layer 2 mapping both eras to shared concept names
- Layer 3 release exposes canonical metrics; era-specific row_keys remain available for audit/research use
- DuckDB query layer introduced (see Data for Frontend)
- Requires: canonical metric crosswalk config, processed.py era-aware subset lists, frontend schema update

**v3.0 — future** (TBD)
- May include: additional years as AG publishes, revised ACS population vintages, expanded GIS outputs

### Data contract

The data contract defines what the frontend can rely on across releases. It lives in this file and is the coordination point when pipeline and frontend agents work in parallel.

**Stable across all versions:**
- Core tidy row schema: `year`, `agency`, `row_key`, `row_id`, `table_id`, `section_id`, `metric_id`, plus race count columns
- Race columns (2020+): `Total`, `White`, `Black`, `Hispanic`, `Native American`, `Asian`, `Other`
- Race columns (pre-2020): `Total`, `White`, `Black`, `Hispanic`, `Asian`, `Am. Indian`, `Other`
- `row_key` pattern: `{table_id}--{section_id}--{metric_id}`
- `row_id` pattern: `{year}-{agency_slug}-{row_key}`

**Added in v2:**
- `canonical_key` — era-independent concept name (e.g., `stops`, `searches`, `arrests`, `moving-violation`, `consent-search`). Null for metrics with no cross-era equivalent.

**Canonical metric crosswalk (draft — v2 design):**

| canonical_key | pre-2020 row_key | 2020+ row_key |
|---|---|---|
| `stops` | `key-indicators--stops` | `rates-by-race--totals--all-stops` |
| `searches` | `key-indicators--searches` | `rates-by-race--totals--searches` |
| `arrests` | `key-indicators--arrests` | `number-of-stops-by-race--stop-outcome--arrests` |
| `moving-violation` | `vehicle-stop-stats--reason-for-stop--moving` | `number-of-stops-by-race--reason-for-stop--moving` |
| `consent-search` | `search-stats--probable-cause-authority-to-search--consent` | `search-statistics--probable-cause--consent` |

Note: some 2020+ metrics have no pre-2020 equivalent (resident-only stops, ACS population rows, citation rate) and vice versa (pre-2020 bakes search rate and contraband hit rate directly; 2020+ derives them from raw counts). The crosswalk config will be the authoritative source; this table is a planning aid.
