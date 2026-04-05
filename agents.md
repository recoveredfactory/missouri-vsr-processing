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
