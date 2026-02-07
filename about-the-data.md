# About the data

## What’s in here

Missouri requires police agencies to report data on every traffic stop to the state Attorney General: who got stopped, why, what happened, and whether a search occurred. We extract that data from yearly PDF reports published by the AG’s office.

This dataset includes counts and rates, broken down by the race of the driver. The metrics tracked include:

- **Stops**: total stops, resident stops, non‑resident stops
- **Outcomes**: arrests, citations, searches, and contraband found
- **Rates**: arrest rate, citation rate, search rate, contraband hit rate, stop rate, resident stop rate
- **Arrest reason** (e.g., drug violation)
- **Citation/warning reason** (e.g., moving violation)
- **Driver age** (17 and under, 18–29, 30–39, 40–64, 65+)
- **Driver gender** (male, female)
- **Location of stop** (city street, county road, interstate, etc.)
- **Officer assignment** (dedicated traffic, general parol, special assignment)
- **Reason for stop** (investigative, moving, equipment, etc.)
- **Stop outcome** (arrest, citation, warning, no action, etc.)
- **Type of contraband found** (drugs, weapons, etc.)
- **Type of search / probable cause** (consent, incident to arrest, etc.)
- **Search duration** (0–15 min, 16–30 min, 31+ min)
- **What was searched** (vehicle property, driver, driver property)
- **Population estimates** from the Decennial Census and the American Community Survey (ACS)

## Where it came from

The Missouri Attorney General is [required by statute](http://revisor.mo.gov/main/OneSection.aspx?section=590.650&bid=30357&hl=) to compile and publish the Vehicle Stops Report.

Each report typically includes an executive summary, statewide aggregates, agency‑specific reports, and a separate document of agency comments (if submitted). We extract structured data from the agency‑specific reports and from the agency responses, which are later joined together.

Source reports and comments are available from the AG’s office:

- [Vehicle Stops Report landing page](https://ago.mo.gov/get-help/vehicle-stops-report/)
- Agency‑specific report PDFs:
  - 2024: https://ago.mo.gov/wp-content/uploads/2024-VSR-Agency-Specific-Reports.pdf
  - 2023: https://ago.mo.gov/wp-content/uploads/VSRreport2023.pdf
  - 2022: https://ago.mo.gov/wp-content/uploads/vsrreport2022.pdf
  - 2021: https://ago.mo.gov/wp-content/uploads/2021-VSR-Agency-Specific-Report.pdf
  - 2020: https://ago.mo.gov/wp-content/uploads/2020-VSR-Agency-Specific-Report.pdf
- Agency response PDFs:
  - 2024: https://ago.mo.gov/wp-content/uploads/2024-Agency-Responses-1.pdf
  - 2023: https://ago.mo.gov/wp-content/uploads/VSRagencynotes2023.pdf
  - 2022: https://ago.mo.gov/wp-content/uploads/2022-agency-comments-ago.pdf
  - 2021: https://ago.mo.gov/wp-content/uploads/2021-VSR-Agency-Comments.pdf
  - 2020: https://ago.mo.gov/wp-content/uploads/2020-VSR-Agency-Comments.pdf

Currently, we extract data for reports **2020–2024** (published 2021–2025).

Agency metadata (names, addresses, contact info) comes from a 2025 copy of the Missouri law enforcement agencies database provided by Jesse Bogan at The Marshall Project. The latest version of this data is [available via data.mo.gov](https://data.mo.gov/Public-Safety/Missouri-Law-Enforcement-Agencies/cgbu-k38b/about_data) and will be integrated after the 2025 VSR is released (spring 2026).

Because agency names vary between the agencies database and the VSR, we built a crosswalk to join their information.

The address from the agency data is run through Geocod.io to attach geographic identifiers for each jurisdiction and to geocode the agency address. These identifiers are joined with [Census cartographic boundary files](https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html) to display jurisdiction maps and spatial relationships.

The processing pipeline is an [open source Python/Dagster project](https://github.com/eads/missouri-vsr-processing) originally developed at The Marshall Project.

## How the data is structured

### Row model (the core table)

The central dataset is a row‑based table where each row represents one metric for one agency and one year. Key fields:

- `agency` — the agency name as it appears in the report
- `year` — report year
- `table`, `section`, `metric` — human‑readable labels
- `table_id`, `section_id`, `metric_id` — slugified identifiers
- `row_key` — `table_id--section_id--metric_id` (stable across table renumbering)
- `row_id` — `year-agency-row_key` (globally unique)
- Race columns: `Total`, `White`, `Black`, `Hispanic`, `Native American`, `Asian`, `Other`

All values are numeric or null (`.` in the PDF becomes `null`).

### Rate rows and normalization

Rates in the reports (e.g., “search rate”) are captured as provided. However:

- **Statewide rates** are **recomputed** from totals for consistency; we do not sum rate rows.
- `YYYY ACS pop` fields are normalized to `acs-pop` so population variables are comparable across years.
- In early years (2020–2021), population rows that look like `YYYY-pop` are also mapped to `acs-pop`.

### Agency comments

Agency comments are parsed from the separate response PDFs and attached downstream. Each comment entry has:

- `agency`
- `year`
- `comment` (string, with paragraph breaks preserved as `\n\n`)
- `has_comment`
- `source_url`

Line breaks inside paragraphs are collapsed to a single space. Paragraph breaks are preserved.

## Downloads and file formats

All public downloads live under:

`https://data.vsr.recoveredfactory.net/`

### Combined JSON (all datasets)

This file contains **all** datasets in one JSON object with keys:

- `vsr_statistics`
- `agency_index`
- `agency_comments`

Download:

- https://data.vsr.recoveredfactory.net/downloads/missouri_vsr_2020_2024_downloads.json

### Per‑dataset CSV + Parquet

For analysis in pandas/R/SQL, each dataset is also provided as CSV and Parquet:

**VSR statistics (with rank/percentile/percentage rows)**

- https://data.vsr.recoveredfactory.net/downloads/missouri_vsr_2020_2024_vsr_statistics.csv
- https://data.vsr.recoveredfactory.net/downloads/missouri_vsr_2020_2024_vsr_statistics.parquet

**Agency index (names + metadata)**

- https://data.vsr.recoveredfactory.net/downloads/missouri_vsr_2020_2024_agency_index.csv
- https://data.vsr.recoveredfactory.net/downloads/missouri_vsr_2020_2024_agency_index.parquet

**Agency comments**

- https://data.vsr.recoveredfactory.net/downloads/missouri_vsr_2020_2024_agency_comments.csv
- https://data.vsr.recoveredfactory.net/downloads/missouri_vsr_2020_2024_agency_comments.parquet

### Download manifest

The manifest includes file sizes for dynamic download UIs:

- https://data.vsr.recoveredfactory.net/downloads/missouri_vsr_2020_2024_downloads_manifest.json

### Statewide sums (subset)

A slim statewide summary used for homepage charts:

- https://data.vsr.recoveredfactory.net/statewide_year_sums_subset.json

Included row_keys:
- rates-by-race--totals--all-stops
- search-statistics--probable-cause--consent
- number-of-stops-by-race--stop-outcome--arrests
- number-of-stops-by-race--stop-outcome--citation
- number-of-stops-by-race--stop-outcome--warning
- number-of-stops-by-race--stop-outcome--no-action

### Per‑agency downloads (convenience)

These files are convenient for single‑agency use, but the **primary** way to get the data is still the combined downloads above.

| Resource | URL pattern | Notes |
|---|---|---|
| Agency year JSON | https://data.vsr.recoveredfactory.net/agency_year/{agency_slug}.json | All rows, metadata, and comments for one agency |
| Agency boundary GeoJSON | https://data.vsr.recoveredfactory.net/agency_boundaries/{agency_id}.geojson | Single FeatureCollection with boundary + neighbors |
| Agency boundary index | https://data.vsr.recoveredfactory.net/dist/agency_boundaries_index.json | Lists which boundary files exist (by slug) |

## Format quirks and implementation notes

### Agency index (`agency_index`)

The agency index is an aggregation of agency metadata and the VSR names. Notable fields:

- `agency_slug` — slugified canonical name (apostrophes removed, punctuation collapsed)
- `names` — **array of known names** for the agency (canonical, crosswalked, and VSR variants)
- `city`, `zip`, `phone`, `county` — from the agency reference database
- `rates-by-race--totals--all-stops` and `all_stops_total` — the most recent total stops, for weighting/search

**CSV quirk:** `names` is serialized as a JSON array string in CSV (e.g. `"[\"Agency A\", \"Agency A PD\"]"`).
In Parquet and JSON, it is a native list.

### Agency comments (`agency_comments`)

- `comment` preserves paragraph breaks as `\n\n`.
- Line breaks within a paragraph are collapsed to spaces.
- Text is minimally cleaned; odd characters present in the source PDFs are preserved.

### Statewide sums (`statewide_year_sums`)

- Rows ending in `-rate` are **excluded from summation**; statewide rates are **recomputed** from totals.
- `no-mshp--*` excludes the Missouri State Highway Patrol.
- `avg-no-mshp--*` is an average across agencies (after excluding MSHP), not a sum.

### VSR statistics (`reports_with_rank_percentile`)

Derived rows are added per metric:

- `-rank` (dense rank, 1 = highest)
- `-percentile` (0–1 scale)
- `-percentage` for non‑rate metrics (race ÷ total)

## Metric definitions (rates)

Rates are defined according to the VSR documentation:

- **Stop rate**: (stops / previous year ACS population) × 100
- **Resident stop rate**: (resident stops / previous year ACS population) × 100
- **Search rate**: (searches / stops) × 100
- **Contraband hit rate**: (searches with contraband found / total searches) × 100
- **Arrest rate**: (arrests / stops) × 100
- **Citation rate**: (citations / stops) × 100

## Metrics tracked (row_key list)

Some metrics appear only in certain years (e.g., disparity index was discontinued after 2022). Misspellings from the original reports are preserved (e.g., “parol”, “equpiment”).

For convenience, each row key has a per‑metric JSON file at:
`https://data.vsr.recoveredfactory.net/metric_year/{row_key}.json`
These are handy for single‑metric use, but the combined downloads are the primary interface.

**All row_keys currently present:**

| Category | Row key | Metric file |
|---|---|---|
| disparity-index-by-race | disparity-index-by-race--disparity-index--all-stops | https://data.vsr.recoveredfactory.net/metric_year/disparity-index-by-race--disparity-index--all-stops.json |
| disparity-index-by-race | disparity-index-by-race--disparity-index--all-stops-acs | https://data.vsr.recoveredfactory.net/metric_year/disparity-index-by-race--disparity-index--all-stops-acs.json |
| disparity-index-by-race | disparity-index-by-race--disparity-index--all-stops-dec | https://data.vsr.recoveredfactory.net/metric_year/disparity-index-by-race--disparity-index--all-stops-dec.json |
| disparity-index-by-race | disparity-index-by-race--disparity-index--resident-stops | https://data.vsr.recoveredfactory.net/metric_year/disparity-index-by-race--disparity-index--resident-stops.json |
| disparity-index-by-race | disparity-index-by-race--disparity-index--resident-stops-acs | https://data.vsr.recoveredfactory.net/metric_year/disparity-index-by-race--disparity-index--resident-stops-acs.json |
| disparity-index-by-race | disparity-index-by-race--disparity-index--resident-stops-dec | https://data.vsr.recoveredfactory.net/metric_year/disparity-index-by-race--disparity-index--resident-stops-dec.json |
| disparity-index-by-race | disparity-index-by-race--population--2019-population | https://data.vsr.recoveredfactory.net/metric_year/disparity-index-by-race--population--2019-population.json |
| disparity-index-by-race | disparity-index-by-race--population--2019-population-pct | https://data.vsr.recoveredfactory.net/metric_year/disparity-index-by-race--population--2019-population-pct.json |
| disparity-index-by-race | disparity-index-by-race--population--2020-decennial-pop | https://data.vsr.recoveredfactory.net/metric_year/disparity-index-by-race--population--2020-decennial-pop.json |
| disparity-index-by-race | disparity-index-by-race--population--2020-decennial-pop-pct | https://data.vsr.recoveredfactory.net/metric_year/disparity-index-by-race--population--2020-decennial-pop-pct.json |
| disparity-index-by-race | disparity-index-by-race--population--2020-population | https://data.vsr.recoveredfactory.net/metric_year/disparity-index-by-race--population--2020-population.json |
| disparity-index-by-race | disparity-index-by-race--population--2020-population-pct | https://data.vsr.recoveredfactory.net/metric_year/disparity-index-by-race--population--2020-population-pct.json |
| disparity-index-by-race | disparity-index-by-race--population--acs-pop | https://data.vsr.recoveredfactory.net/metric_year/disparity-index-by-race--population--acs-pop.json |
| disparity-index-by-race | disparity-index-by-race--population--acs-pop-pct | https://data.vsr.recoveredfactory.net/metric_year/disparity-index-by-race--population--acs-pop-pct.json |
| disparity-index-by-race | disparity-index-by-race--resident-stops-acs--all-stops-dec | https://data.vsr.recoveredfactory.net/metric_year/disparity-index-by-race--resident-stops-acs--all-stops-dec.json |
| disparity-index-by-race | disparity-index-by-race--resident-stops-acs--resident-stops-dec | https://data.vsr.recoveredfactory.net/metric_year/disparity-index-by-race--resident-stops-acs--resident-stops-dec.json |
| disparity-index-by-race | disparity-index-by-race--stops--all-stops | https://data.vsr.recoveredfactory.net/metric_year/disparity-index-by-race--stops--all-stops.json |
| disparity-index-by-race | disparity-index-by-race--stops--resident-stops | https://data.vsr.recoveredfactory.net/metric_year/disparity-index-by-race--stops--resident-stops.json |
| number-of-stops-by-race | number-of-stops-by-race--all-stops | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--all-stops.json |
| number-of-stops-by-race | number-of-stops-by-race--arrest-violation--drug-violation | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--arrest-violation--drug-violation.json |
| number-of-stops-by-race | number-of-stops-by-race--arrest-violation--dwi-bac | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--arrest-violation--dwi-bac.json |
| number-of-stops-by-race | number-of-stops-by-race--arrest-violation--off-against-person | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--arrest-violation--off-against-person.json |
| number-of-stops-by-race | number-of-stops-by-race--arrest-violation--other | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--arrest-violation--other.json |
| number-of-stops-by-race | number-of-stops-by-race--arrest-violation--outstanding-warrent | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--arrest-violation--outstanding-warrent.json |
| number-of-stops-by-race | number-of-stops-by-race--arrest-violation--property | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--arrest-violation--property.json |
| number-of-stops-by-race | number-of-stops-by-race--arrest-violation--resist-arrest | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--arrest-violation--resist-arrest.json |
| number-of-stops-by-race | number-of-stops-by-race--arrest-violation--traffic | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--arrest-violation--traffic.json |
| number-of-stops-by-race | number-of-stops-by-race--citation-warning-violation--equipment | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--citation-warning-violation--equipment.json |
| number-of-stops-by-race | number-of-stops-by-race--citation-warning-violation--license-registration | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--citation-warning-violation--license-registration.json |
| number-of-stops-by-race | number-of-stops-by-race--citation-warning-violation--moving | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--citation-warning-violation--moving.json |
| number-of-stops-by-race | number-of-stops-by-race--driver-age--17-and-under | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--driver-age--17-and-under.json |
| number-of-stops-by-race | number-of-stops-by-race--driver-age--18-29 | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--driver-age--18-29.json |
| number-of-stops-by-race | number-of-stops-by-race--driver-age--30-39 | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--driver-age--30-39.json |
| number-of-stops-by-race | number-of-stops-by-race--driver-age--40-64 | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--driver-age--40-64.json |
| number-of-stops-by-race | number-of-stops-by-race--driver-age--40-and-over | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--driver-age--40-and-over.json |
| number-of-stops-by-race | number-of-stops-by-race--driver-age--65-and-over | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--driver-age--65-and-over.json |
| number-of-stops-by-race | number-of-stops-by-race--driver-gender--female | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--driver-gender--female.json |
| number-of-stops-by-race | number-of-stops-by-race--driver-gender--male | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--driver-gender--male.json |
| number-of-stops-by-race | number-of-stops-by-race--location-of-stop--city-street | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--location-of-stop--city-street.json |
| number-of-stops-by-race | number-of-stops-by-race--location-of-stop--county-road | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--location-of-stop--county-road.json |
| number-of-stops-by-race | number-of-stops-by-race--location-of-stop--interstate-hwy | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--location-of-stop--interstate-hwy.json |
| number-of-stops-by-race | number-of-stops-by-race--location-of-stop--other | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--location-of-stop--other.json |
| number-of-stops-by-race | number-of-stops-by-race--location-of-stop--state-hwy | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--location-of-stop--state-hwy.json |
| number-of-stops-by-race | number-of-stops-by-race--location-of-stop--us-hwy | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--location-of-stop--us-hwy.json |
| number-of-stops-by-race | number-of-stops-by-race--non-resident-stops | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--non-resident-stops.json |
| number-of-stops-by-race | number-of-stops-by-race--officer-assignment--dedicated-traffic | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--officer-assignment--dedicated-traffic.json |
| number-of-stops-by-race | number-of-stops-by-race--officer-assignment--general-parol | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--officer-assignment--general-parol.json |
| number-of-stops-by-race | number-of-stops-by-race--officer-assignment--special-assignment | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--officer-assignment--special-assignment.json |
| number-of-stops-by-race | number-of-stops-by-race--reason-for-stop--called-for-service | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--reason-for-stop--called-for-service.json |
| number-of-stops-by-race | number-of-stops-by-race--reason-for-stop--det-crime-bulletin | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--reason-for-stop--det-crime-bulletin.json |
| number-of-stops-by-race | number-of-stops-by-race--reason-for-stop--equpiment | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--reason-for-stop--equpiment.json |
| number-of-stops-by-race | number-of-stops-by-race--reason-for-stop--investigative | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--reason-for-stop--investigative.json |
| number-of-stops-by-race | number-of-stops-by-race--reason-for-stop--license | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--reason-for-stop--license.json |
| number-of-stops-by-race | number-of-stops-by-race--reason-for-stop--moving | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--reason-for-stop--moving.json |
| number-of-stops-by-race | number-of-stops-by-race--reason-for-stop--officer-initiative | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--reason-for-stop--officer-initiative.json |
| number-of-stops-by-race | number-of-stops-by-race--reason-for-stop--other | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--reason-for-stop--other.json |
| number-of-stops-by-race | number-of-stops-by-race--resident-stops | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--resident-stops.json |
| number-of-stops-by-race | number-of-stops-by-race--stop-outcome--arrests | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--stop-outcome--arrests.json |
| number-of-stops-by-race | number-of-stops-by-race--stop-outcome--citation | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--stop-outcome--citation.json |
| number-of-stops-by-race | number-of-stops-by-race--stop-outcome--contraband | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--stop-outcome--contraband.json |
| number-of-stops-by-race | number-of-stops-by-race--stop-outcome--no-action | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--stop-outcome--no-action.json |
| number-of-stops-by-race | number-of-stops-by-race--stop-outcome--searches | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--stop-outcome--searches.json |
| number-of-stops-by-race | number-of-stops-by-race--stop-outcome--warning | https://data.vsr.recoveredfactory.net/metric_year/number-of-stops-by-race--stop-outcome--warning.json |
| rates-by-race | rates-by-race--contraband-hit-rate--arrest-rate | https://data.vsr.recoveredfactory.net/metric_year/rates-by-race--contraband-hit-rate--arrest-rate.json |
| rates-by-race | rates-by-race--contraband-hit-rate--citation-rate | https://data.vsr.recoveredfactory.net/metric_year/rates-by-race--contraband-hit-rate--citation-rate.json |
| rates-by-race | rates-by-race--population--2019-population | https://data.vsr.recoveredfactory.net/metric_year/rates-by-race--population--2019-population.json |
| rates-by-race | rates-by-race--population--2019-population-pct | https://data.vsr.recoveredfactory.net/metric_year/rates-by-race--population--2019-population-pct.json |
| rates-by-race | rates-by-race--population--2020-decennial-pop | https://data.vsr.recoveredfactory.net/metric_year/rates-by-race--population--2020-decennial-pop.json |
| rates-by-race | rates-by-race--population--2020-decennial-pop-pct | https://data.vsr.recoveredfactory.net/metric_year/rates-by-race--population--2020-decennial-pop-pct.json |
| rates-by-race | rates-by-race--population--2020-population | https://data.vsr.recoveredfactory.net/metric_year/rates-by-race--population--2020-population.json |
| rates-by-race | rates-by-race--population--2020-population-pct | https://data.vsr.recoveredfactory.net/metric_year/rates-by-race--population--2020-population-pct.json |
| rates-by-race | rates-by-race--population--acs-pop | https://data.vsr.recoveredfactory.net/metric_year/rates-by-race--population--acs-pop.json |
| rates-by-race | rates-by-race--population--acs-pop-pct | https://data.vsr.recoveredfactory.net/metric_year/rates-by-race--population--acs-pop-pct.json |
| rates-by-race | rates-by-race--rates--arrest-rate | https://data.vsr.recoveredfactory.net/metric_year/rates-by-race--rates--arrest-rate.json |
| rates-by-race | rates-by-race--rates--citation-rate | https://data.vsr.recoveredfactory.net/metric_year/rates-by-race--rates--citation-rate.json |
| rates-by-race | rates-by-race--rates--contraband-hit-rate | https://data.vsr.recoveredfactory.net/metric_year/rates-by-race--rates--contraband-hit-rate.json |
| rates-by-race | rates-by-race--rates--search-rate | https://data.vsr.recoveredfactory.net/metric_year/rates-by-race--rates--search-rate.json |
| rates-by-race | rates-by-race--rates--stop-rate | https://data.vsr.recoveredfactory.net/metric_year/rates-by-race--rates--stop-rate.json |
| rates-by-race | rates-by-race--rates--stop-rate-residents | https://data.vsr.recoveredfactory.net/metric_year/rates-by-race--rates--stop-rate-residents.json |
| rates-by-race | rates-by-race--stop-rate-residents--arrest-rate | https://data.vsr.recoveredfactory.net/metric_year/rates-by-race--stop-rate-residents--arrest-rate.json |
| rates-by-race | rates-by-race--stop-rate-residents--citation-rate | https://data.vsr.recoveredfactory.net/metric_year/rates-by-race--stop-rate-residents--citation-rate.json |
| rates-by-race | rates-by-race--stop-rate-residents--contraband-hit-rate | https://data.vsr.recoveredfactory.net/metric_year/rates-by-race--stop-rate-residents--contraband-hit-rate.json |
| rates-by-race | rates-by-race--stop-rate-residents--search-rate | https://data.vsr.recoveredfactory.net/metric_year/rates-by-race--stop-rate-residents--search-rate.json |
| rates-by-race | rates-by-race--totals--all-stops | https://data.vsr.recoveredfactory.net/metric_year/rates-by-race--totals--all-stops.json |
| rates-by-race | rates-by-race--totals--arrests | https://data.vsr.recoveredfactory.net/metric_year/rates-by-race--totals--arrests.json |
| rates-by-race | rates-by-race--totals--citations | https://data.vsr.recoveredfactory.net/metric_year/rates-by-race--totals--citations.json |
| rates-by-race | rates-by-race--totals--contraband | https://data.vsr.recoveredfactory.net/metric_year/rates-by-race--totals--contraband.json |
| rates-by-race | rates-by-race--totals--resident-stops | https://data.vsr.recoveredfactory.net/metric_year/rates-by-race--totals--resident-stops.json |
| rates-by-race | rates-by-race--totals--searches | https://data.vsr.recoveredfactory.net/metric_year/rates-by-race--totals--searches.json |
| search-statistics | search-statistics--arrest-charge--drug-violation | https://data.vsr.recoveredfactory.net/metric_year/search-statistics--arrest-charge--drug-violation.json |
| search-statistics | search-statistics--arrest-charge--dwi-bac | https://data.vsr.recoveredfactory.net/metric_year/search-statistics--arrest-charge--dwi-bac.json |
| search-statistics | search-statistics--arrest-charge--off-against-person | https://data.vsr.recoveredfactory.net/metric_year/search-statistics--arrest-charge--off-against-person.json |
| search-statistics | search-statistics--arrest-charge--other | https://data.vsr.recoveredfactory.net/metric_year/search-statistics--arrest-charge--other.json |
| search-statistics | search-statistics--arrest-charge--outstanding-warrant | https://data.vsr.recoveredfactory.net/metric_year/search-statistics--arrest-charge--outstanding-warrant.json |
| search-statistics | search-statistics--contraband-found--alcohol | https://data.vsr.recoveredfactory.net/metric_year/search-statistics--contraband-found--alcohol.json |
| search-statistics | search-statistics--contraband-found--drugs | https://data.vsr.recoveredfactory.net/metric_year/search-statistics--contraband-found--drugs.json |
| search-statistics | search-statistics--contraband-found--other | https://data.vsr.recoveredfactory.net/metric_year/search-statistics--contraband-found--other.json |
| search-statistics | search-statistics--contraband-found--weapons | https://data.vsr.recoveredfactory.net/metric_year/search-statistics--contraband-found--weapons.json |
| search-statistics | search-statistics--search-duration--0-15-min | https://data.vsr.recoveredfactory.net/metric_year/search-statistics--search-duration--0-15-min.json |
| search-statistics | search-statistics--search-duration--16-30-min | https://data.vsr.recoveredfactory.net/metric_year/search-statistics--search-duration--16-30-min.json |
| search-statistics | search-statistics--search-duration--31-min-or-more | https://data.vsr.recoveredfactory.net/metric_year/search-statistics--search-duration--31-min-or-more.json |
| search-statistics | search-statistics--search-duration--unknown | https://data.vsr.recoveredfactory.net/metric_year/search-statistics--search-duration--unknown.json |
| search-statistics | search-statistics--search-reason--consent | https://data.vsr.recoveredfactory.net/metric_year/search-statistics--search-reason--consent.json |
| search-statistics | search-statistics--search-reason--frisked | https://data.vsr.recoveredfactory.net/metric_year/search-statistics--search-reason--frisked.json |
| search-statistics | search-statistics--search-reason--incident-to-arrest | https://data.vsr.recoveredfactory.net/metric_year/search-statistics--search-reason--incident-to-arrest.json |
| search-statistics | search-statistics--search-reason--inventory | https://data.vsr.recoveredfactory.net/metric_year/search-statistics--search-reason--inventory.json |
| search-statistics | search-statistics--search-reason--other | https://data.vsr.recoveredfactory.net/metric_year/search-statistics--search-reason--other.json |
| search-statistics | search-statistics--search-reason--probable-cause | https://data.vsr.recoveredfactory.net/metric_year/search-statistics--search-reason--probable-cause.json |
| search-statistics | search-statistics--search-reason--probable-cause-vehicle | https://data.vsr.recoveredfactory.net/metric_year/search-statistics--search-reason--probable-cause-vehicle.json |
| search-statistics | search-statistics--search-reason--probable-cause-vehicle-person | https://data.vsr.recoveredfactory.net/metric_year/search-statistics--search-reason--probable-cause-vehicle-person.json |
| search-statistics | search-statistics--search-reason--search-warrant | https://data.vsr.recoveredfactory.net/metric_year/search-statistics--search-reason--search-warrant.json |
| search-statistics | search-statistics--search-reason--special-circumstances | https://data.vsr.recoveredfactory.net/metric_year/search-statistics--search-reason--special-circumstances.json |
| search-statistics | search-statistics--searched--driver | https://data.vsr.recoveredfactory.net/metric_year/search-statistics--searched--driver.json |
| search-statistics | search-statistics--searched--driver-property | https://data.vsr.recoveredfactory.net/metric_year/search-statistics--searched--driver-property.json |
| search-statistics | search-statistics--searched--vehicle | https://data.vsr.recoveredfactory.net/metric_year/search-statistics--searched--vehicle.json |
| search-statistics | search-statistics--searched--vehicle-property | https://data.vsr.recoveredfactory.net/metric_year/search-statistics--searched--vehicle-property.json |
