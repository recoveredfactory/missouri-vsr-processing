# About the data

## What’s in here

Missouri requires police agencies to report data on every traffic stop to the state Attorney General: who got stopped, why, what happened, and whether the vehicle was searched. We extract that data from yearly PDF reports published by the AG’s office.

This dataset includes a variety of counts and rates, broken down by the race of the driver.

The metrics tracked include:

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

Currently, we extract data for reports **2020–2024** (published 2021–2025).

Agency metadata (names, addresses, contact info) comes from a 2025 copy of the Missouri law enforcement agencies database provided by Jesse Bogan at The Marshall Project. The latest version of this data is [available via data.mo.gov](https://data.mo.gov/Public-Safety/Missouri-Law-Enforcement-Agencies/cgbu-k38b/about_data) and will be integrated after the 2025 VSR is released (spring 2026).

Because agency names vary between the agencies database and the VSR, we built a crosswalk to join their information.

The address from the agency data is then run through Geocod.io to attach geographic identifiers for each jurisdiction and to geocode the agency address. These identifiers are joined with [Census cartographic boundary files](https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html) to display jurisdiction maps and spatial relationships.

The processing pipeline is an [open source Python/Dagster project](https://github.com/eads/missouri-vsr-processing) originally developed at The Marshall Project.

## How to use it

Search by agency name to see the data extracted from their VSR. Click metrics to compare against statewide numbers and see trends over time.

In addition to the main data download, historical data for each agency can be downloaded from the agency’s detail page. This is intended to allow journalists and researchers to work with a single agency without handling the full statewide dataset.

### Using the full dataset

The full dataset is a row‑based table for analysis (no agency comments inline). It’s best for statewide analysis, custom aggregations, and dashboards. Agency comments are stored separately.

### Using the agency‑year files

Each agency has a JSON file with all years of data, plus metadata and (if available) agency comments.

**Skeleton example:**
```json
{
  "agency": "Adair County Sheriff's Dept",
  "agency_metadata": {
    "AddressCity": "Kirksville",
    "Phone": "660-665-3340",
    "...": "..."
  },
  "agency_comments": [
    {
      "year": 2024,
      "comment": "…full comment text…",
      "has_comment": true,
      "source_url": "https://ago.mo.gov/wp-content/uploads/2024-Agency-Responses-1.pdf"
    }
  ],
  "rows": [
    {
      "year": 2024,
      "row_key": "rates-by-race--totals--all-stops",
      "table": "Rates by Race",
      "table_id": "rates-by-race",
      "section": "Totals",
      "section_id": "totals",
      "metric": "All stops",
      "metric_id": "all-stops",
      "row_id": "2024-adair-county-sheriff-s-dept-rates-by-race--totals--all-stops",
      "Total": 317,
      "White": 281,
      "Black": 25,
      "Hispanic": 8,
      "Native American": 0,
      "Asian": 2,
      "Other": 1
    }
  ]
}
```

## Files and formats (current)

- **Full combined table**: `data/processed/all_combined_output.parquet`
  Row‑based, includes `row_key`, `table_id`, `section_id`, `metric_id`, and race columns.
- **VSR statistics (with rank/percentile/percentage rows)**: `data/processed/reports_with_rank_percentile.parquet`
- **Agency comments**: `data/processed/agency_comments.parquet`
- **Per‑agency JSON**: `data/out/agency_year/<agency_slug>.json`
- **Per‑metric JSON**: `data/out/metric_year/<row_key>.json`
- **Compact per‑metric JSON**: `data/out/metric_year_subset.json`
  Indexed format with `agencies`, `years`, and `rows[row_key]`.
- **Statewide baselines**: `data/out/statewide_slug_baselines.json`
- **Statewide per‑year sums**: `data/out/statewide_year_sums.json`
  Includes `no-mshp--*` and `avg-no-mshp--*` variants and recomputed rate rows.
- **Report dimension index**: `data/out/report_dimensions.json`
- **Agency boundaries**: `data/out/agency_boundaries/<agency_id>.geojson`
- **PMTiles**: `data/out/tiles/mo_jurisdictions_2024_500k.pmtiles`

**Download bundles** live in `data/out/downloads/` and are prefixed with
`missouri_vsr_<min_year>_<max_year>_` (for the current data: `missouri_vsr_2020_2024_`).

One **combined** download file is provided:

- `missouri_vsr_2020_2024_downloads.json`

CSV + Parquet are also provided for each dataset:

- `missouri_vsr_2020_2024_vsr_statistics.{csv,parquet}`
- `missouri_vsr_2020_2024_agency_index.{csv,parquet}`
- `missouri_vsr_2020_2024_agency_comments.{csv,parquet}`

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

**All row_keys currently present:**

| Category | Row key |
|---|---|
| disparity-index-by-race | disparity-index-by-race--disparity-index--all-stops |
| disparity-index-by-race | disparity-index-by-race--disparity-index--all-stops-acs |
| disparity-index-by-race | disparity-index-by-race--disparity-index--all-stops-dec |
| disparity-index-by-race | disparity-index-by-race--disparity-index--resident-stops |
| disparity-index-by-race | disparity-index-by-race--disparity-index--resident-stops-acs |
| disparity-index-by-race | disparity-index-by-race--disparity-index--resident-stops-dec |
| disparity-index-by-race | disparity-index-by-race--population--2019-population |
| disparity-index-by-race | disparity-index-by-race--population--2019-population-pct |
| disparity-index-by-race | disparity-index-by-race--population--2020-decennial-pop |
| disparity-index-by-race | disparity-index-by-race--population--2020-decennial-pop-pct |
| disparity-index-by-race | disparity-index-by-race--population--2020-population |
| disparity-index-by-race | disparity-index-by-race--population--2020-population-pct |
| disparity-index-by-race | disparity-index-by-race--population--acs-pop |
| disparity-index-by-race | disparity-index-by-race--population--acs-pop-pct |
| disparity-index-by-race | disparity-index-by-race--resident-stops-acs--all-stops-dec |
| disparity-index-by-race | disparity-index-by-race--resident-stops-acs--resident-stops-dec |
| disparity-index-by-race | disparity-index-by-race--stops--all-stops |
| disparity-index-by-race | disparity-index-by-race--stops--resident-stops |
| number-of-stops-by-race | number-of-stops-by-race--all-stops |
| number-of-stops-by-race | number-of-stops-by-race--arrest-violation--drug-violation |
| number-of-stops-by-race | number-of-stops-by-race--arrest-violation--dwi-bac |
| number-of-stops-by-race | number-of-stops-by-race--arrest-violation--off-against-person |
| number-of-stops-by-race | number-of-stops-by-race--arrest-violation--other |
| number-of-stops-by-race | number-of-stops-by-race--arrest-violation--outstanding-warrent |
| number-of-stops-by-race | number-of-stops-by-race--arrest-violation--property |
| number-of-stops-by-race | number-of-stops-by-race--arrest-violation--resist-arrest |
| number-of-stops-by-race | number-of-stops-by-race--arrest-violation--traffic |
| number-of-stops-by-race | number-of-stops-by-race--citation-warning-violation--equipment |
| number-of-stops-by-race | number-of-stops-by-race--citation-warning-violation--license-registration |
| number-of-stops-by-race | number-of-stops-by-race--citation-warning-violation--moving |
| number-of-stops-by-race | number-of-stops-by-race--driver-age--17-and-under |
| number-of-stops-by-race | number-of-stops-by-race--driver-age--18-29 |
| number-of-stops-by-race | number-of-stops-by-race--driver-age--30-39 |
| number-of-stops-by-race | number-of-stops-by-race--driver-age--40-64 |
| number-of-stops-by-race | number-of-stops-by-race--driver-age--40-and-over |
| number-of-stops-by-race | number-of-stops-by-race--driver-age--65-and-over |
| number-of-stops-by-race | number-of-stops-by-race--driver-gender--female |
| number-of-stops-by-race | number-of-stops-by-race--driver-gender--male |
| number-of-stops-by-race | number-of-stops-by-race--location-of-stop--city-street |
| number-of-stops-by-race | number-of-stops-by-race--location-of-stop--county-road |
| number-of-stops-by-race | number-of-stops-by-race--location-of-stop--interstate-hwy |
| number-of-stops-by-race | number-of-stops-by-race--location-of-stop--other |
| number-of-stops-by-race | number-of-stops-by-race--location-of-stop--state-hwy |
| number-of-stops-by-race | number-of-stops-by-race--location-of-stop--us-hwy |
| number-of-stops-by-race | number-of-stops-by-race--non-resident-stops |
| number-of-stops-by-race | number-of-stops-by-race--officer-assignment--dedicated-traffic |
| number-of-stops-by-race | number-of-stops-by-race--officer-assignment--general-parol |
| number-of-stops-by-race | number-of-stops-by-race--officer-assignment--special-assignment |
| number-of-stops-by-race | number-of-stops-by-race--reason-for-stop--called-for-service |
| number-of-stops-by-race | number-of-stops-by-race--reason-for-stop--det-crime-bulletin |
| number-of-stops-by-race | number-of-stops-by-race--reason-for-stop--equpiment |
| number-of-stops-by-race | number-of-stops-by-race--reason-for-stop--investigative |
| number-of-stops-by-race | number-of-stops-by-race--reason-for-stop--license |
| number-of-stops-by-race | number-of-stops-by-race--reason-for-stop--moving |
| number-of-stops-by-race | number-of-stops-by-race--reason-for-stop--officer-initiative |
| number-of-stops-by-race | number-of-stops-by-race--reason-for-stop--other |
| number-of-stops-by-race | number-of-stops-by-race--resident-stops |
| number-of-stops-by-race | number-of-stops-by-race--stop-outcome--arrests |
| number-of-stops-by-race | number-of-stops-by-race--stop-outcome--citation |
| number-of-stops-by-race | number-of-stops-by-race--stop-outcome--contraband |
| number-of-stops-by-race | number-of-stops-by-race--stop-outcome--no-action |
| number-of-stops-by-race | number-of-stops-by-race--stop-outcome--searches |
| number-of-stops-by-race | number-of-stops-by-race--stop-outcome--warning |
| rates-by-race | rates-by-race--contraband-hit-rate--arrest-rate |
| rates-by-race | rates-by-race--contraband-hit-rate--citation-rate |
| rates-by-race | rates-by-race--population--2019-population |
| rates-by-race | rates-by-race--population--2019-population-pct |
| rates-by-race | rates-by-race--population--2020-decennial-pop |
| rates-by-race | rates-by-race--population--2020-decennial-pop-pct |
| rates-by-race | rates-by-race--population--2020-population |
| rates-by-race | rates-by-race--population--2020-population-pct |
| rates-by-race | rates-by-race--population--acs-pop |
| rates-by-race | rates-by-race--population--acs-pop-pct |
| rates-by-race | rates-by-race--rates--arrest-rate |
| rates-by-race | rates-by-race--rates--citation-rate |
| rates-by-race | rates-by-race--rates--contraband-hit-rate |
| rates-by-race | rates-by-race--rates--search-rate |
| rates-by-race | rates-by-race--rates--stop-rate |
| rates-by-race | rates-by-race--rates--stop-rate-residents |
| rates-by-race | rates-by-race--stop-rate-residents--arrest-rate |
| rates-by-race | rates-by-race--stop-rate-residents--citation-rate |
| rates-by-race | rates-by-race--stop-rate-residents--contraband-hit-rate |
| rates-by-race | rates-by-race--stop-rate-residents--search-rate |
| rates-by-race | rates-by-race--totals--all-stops |
| rates-by-race | rates-by-race--totals--arrests |
| rates-by-race | rates-by-race--totals--citations |
| rates-by-race | rates-by-race--totals--contraband |
| rates-by-race | rates-by-race--totals--resident-stops |
| rates-by-race | rates-by-race--totals--searches |
| search-statistics | search-statistics--arrest-charge--drug-violation |
| search-statistics | search-statistics--arrest-charge--dwi-bac |
| search-statistics | search-statistics--arrest-charge--off-against-person |
| search-statistics | search-statistics--arrest-charge--other |
| search-statistics | search-statistics--arrest-charge--outstanding-warrant |
| search-statistics | search-statistics--arrest-charge--property-offense |
| search-statistics | search-statistics--arrest-charge--resist-arrest |
| search-statistics | search-statistics--arrest-charge--traffic-violation |
| search-statistics | search-statistics--contraband-found--alcohol |
| search-statistics | search-statistics--contraband-found--currency |
| search-statistics | search-statistics--contraband-found--drugs |
| search-statistics | search-statistics--contraband-found--drugs-alcohol |
| search-statistics | search-statistics--contraband-found--other |
| search-statistics | search-statistics--contraband-found--stolen-property |
| search-statistics | search-statistics--contraband-found--weapon |
| search-statistics | search-statistics--probable-cause--consent |
| search-statistics | search-statistics--probable-cause--drug-alcohol-odor |
| search-statistics | search-statistics--probable-cause--drug-dog-alert |
| search-statistics | search-statistics--probable-cause--incident-to-arrest |
| search-statistics | search-statistics--probable-cause--inventory |
| search-statistics | search-statistics--probable-cause--other |
| search-statistics | search-statistics--probable-cause--plain-view-contra |
| search-statistics | search-statistics--probable-cause--reas-susp-weapon |
| search-statistics | search-statistics--search-duration--0-15-minutes |
| search-statistics | search-statistics--search-duration--16-30-minutes |
| search-statistics | search-statistics--search-duration--31-minutes |
| search-statistics | search-statistics--what-searched--car-property |
| search-statistics | search-statistics--what-searched--driver |
| search-statistics | search-statistics--what-searched--driver-property |
