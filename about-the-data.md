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

### Agency year files (index)

Each link points to that agency’s full JSON file.

<!-- AGENCY_SLUGS_START -->
- adair-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/adair-county-sheriffs-dept.json
- adrian-police-dept: https://data.vsr.recoveredfactory.net/agency_year/adrian-police-dept.json
- advance-police-dept: https://data.vsr.recoveredfactory.net/agency_year/advance-police-dept.json
- alma-police-dept: https://data.vsr.recoveredfactory.net/agency_year/alma-police-dept.json
- alton-police-dept: https://data.vsr.recoveredfactory.net/agency_year/alton-police-dept.json
- anderson-police-dept: https://data.vsr.recoveredfactory.net/agency_year/anderson-police-dept.json
- andrew-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/andrew-county-sheriffs-dept.json
- annapolis-police-dept: https://data.vsr.recoveredfactory.net/agency_year/annapolis-police-dept.json
- appleton-city-police-dept: https://data.vsr.recoveredfactory.net/agency_year/appleton-city-police-dept.json
- arbyrd-police-dept: https://data.vsr.recoveredfactory.net/agency_year/arbyrd-police-dept.json
- arcadia-police-dept: https://data.vsr.recoveredfactory.net/agency_year/arcadia-police-dept.json
- archie-police-dept: https://data.vsr.recoveredfactory.net/agency_year/archie-police-dept.json
- arnold-police-dept: https://data.vsr.recoveredfactory.net/agency_year/arnold-police-dept.json
- ash-grove-police-dept: https://data.vsr.recoveredfactory.net/agency_year/ash-grove-police-dept.json
- ashland-police-dept: https://data.vsr.recoveredfactory.net/agency_year/ashland-police-dept.json
- atchison-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/atchison-county-sheriffs-dept.json
- audrain-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/audrain-county-sheriffs-dept.json
- aurora-police-dept: https://data.vsr.recoveredfactory.net/agency_year/aurora-police-dept.json
- auxvasse-police-dept: https://data.vsr.recoveredfactory.net/agency_year/auxvasse-police-dept.json
- ava-police-dept: https://data.vsr.recoveredfactory.net/agency_year/ava-police-dept.json
- ballwin-police-dept: https://data.vsr.recoveredfactory.net/agency_year/ballwin-police-dept.json
- barry-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/barry-county-sheriffs-dept.json
- barton-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/barton-county-sheriffs-dept.json
- bates-city-police-dept: https://data.vsr.recoveredfactory.net/agency_year/bates-city-police-dept.json
- bates-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/bates-county-sheriffs-dept.json
- battlefield-police-dept: https://data.vsr.recoveredfactory.net/agency_year/battlefield-police-dept.json
- bel-nor-police-dept: https://data.vsr.recoveredfactory.net/agency_year/bel-nor-police-dept.json
- bel-ridge-police-dept: https://data.vsr.recoveredfactory.net/agency_year/bel-ridge-police-dept.json
- bella-villa-police-dept: https://data.vsr.recoveredfactory.net/agency_year/bella-villa-police-dept.json
- belle-police-dept: https://data.vsr.recoveredfactory.net/agency_year/belle-police-dept.json
- bellefontaine-neighbors-police-dept: https://data.vsr.recoveredfactory.net/agency_year/bellefontaine-neighbors-police-dept.json
- bellflower-police-dept: https://data.vsr.recoveredfactory.net/agency_year/bellflower-police-dept.json
- belton-police-dept: https://data.vsr.recoveredfactory.net/agency_year/belton-police-dept.json
- benton-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/benton-county-sheriffs-dept.json
- benton-police-dept: https://data.vsr.recoveredfactory.net/agency_year/benton-police-dept.json
- berger-police-dept: https://data.vsr.recoveredfactory.net/agency_year/berger-police-dept.json
- berkeley-police-dept: https://data.vsr.recoveredfactory.net/agency_year/berkeley-police-dept.json
- bernie-police-dept: https://data.vsr.recoveredfactory.net/agency_year/bernie-police-dept.json
- bertrand-police-dept: https://data.vsr.recoveredfactory.net/agency_year/bertrand-police-dept.json
- bethany-police-dept: https://data.vsr.recoveredfactory.net/agency_year/bethany-police-dept.json
- beverly-hills-police-dept: https://data.vsr.recoveredfactory.net/agency_year/beverly-hills-police-dept.json
- billings-police-dept: https://data.vsr.recoveredfactory.net/agency_year/billings-police-dept.json
- birch-tree-police-dept: https://data.vsr.recoveredfactory.net/agency_year/birch-tree-police-dept.json
- bismarck-police-dept: https://data.vsr.recoveredfactory.net/agency_year/bismarck-police-dept.json
- bland-police-dept: https://data.vsr.recoveredfactory.net/agency_year/bland-police-dept.json
- bloomfield-police-dept: https://data.vsr.recoveredfactory.net/agency_year/bloomfield-police-dept.json
- blue-springs-police-dept: https://data.vsr.recoveredfactory.net/agency_year/blue-springs-police-dept.json
- bolivar-police-dept: https://data.vsr.recoveredfactory.net/agency_year/bolivar-police-dept.json
- bollinger-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/bollinger-county-sheriffs-dept.json
- bonne-terre-police-dept: https://data.vsr.recoveredfactory.net/agency_year/bonne-terre-police-dept.json
- boone-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/boone-county-sheriffs-dept.json
- boonville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/boonville-police-dept.json
- bourbon-dept-of-public-safety: https://data.vsr.recoveredfactory.net/agency_year/bourbon-dept-of-public-safety.json
- bowling-green-police-dept: https://data.vsr.recoveredfactory.net/agency_year/bowling-green-police-dept.json
- branson-police-dept: https://data.vsr.recoveredfactory.net/agency_year/branson-police-dept.json
- branson-west-police-dept: https://data.vsr.recoveredfactory.net/agency_year/branson-west-police-dept.json
- braymer-police-dept: https://data.vsr.recoveredfactory.net/agency_year/braymer-police-dept.json
- breckenridge-hills-police-dept: https://data.vsr.recoveredfactory.net/agency_year/breckenridge-hills-police-dept.json
- brentwood-police-dept: https://data.vsr.recoveredfactory.net/agency_year/brentwood-police-dept.json
- bridgeton-police-dept: https://data.vsr.recoveredfactory.net/agency_year/bridgeton-police-dept.json
- brookfield-police-dept: https://data.vsr.recoveredfactory.net/agency_year/brookfield-police-dept.json
- brunswick-police-dept: https://data.vsr.recoveredfactory.net/agency_year/brunswick-police-dept.json
- buchanan-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/buchanan-county-sheriffs-dept.json
- buckner-police-dept: https://data.vsr.recoveredfactory.net/agency_year/buckner-police-dept.json
- buffalo-police-dept: https://data.vsr.recoveredfactory.net/agency_year/buffalo-police-dept.json
- butler-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/butler-county-sheriffs-dept.json
- butler-police-dept: https://data.vsr.recoveredfactory.net/agency_year/butler-police-dept.json
- butterfield-police-dept: https://data.vsr.recoveredfactory.net/agency_year/butterfield-police-dept.json
- byrnes-mill-police-dept: https://data.vsr.recoveredfactory.net/agency_year/byrnes-mill-police-dept.json
- cabool-police-dept: https://data.vsr.recoveredfactory.net/agency_year/cabool-police-dept.json
- caldwell-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/caldwell-county-sheriffs-dept.json
- callaway-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/callaway-county-sheriffs-dept.json
- calverton-park-police-dept: https://data.vsr.recoveredfactory.net/agency_year/calverton-park-police-dept.json
- camden-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/camden-county-sheriffs-dept.json
- camden-police-dept: https://data.vsr.recoveredfactory.net/agency_year/camden-police-dept.json
- camdenton-police-dept: https://data.vsr.recoveredfactory.net/agency_year/camdenton-police-dept.json
- cameron-police-dept: https://data.vsr.recoveredfactory.net/agency_year/cameron-police-dept.json
- campbell-police-dept: https://data.vsr.recoveredfactory.net/agency_year/campbell-police-dept.json
- canadian-pacific-kansas-city-ltd-police-dept: https://data.vsr.recoveredfactory.net/agency_year/canadian-pacific-kansas-city-ltd-police-dept.json
- canalou-police-dept: https://data.vsr.recoveredfactory.net/agency_year/canalou-police-dept.json
- canton-police-dept: https://data.vsr.recoveredfactory.net/agency_year/canton-police-dept.json
- cape-girardeau-co-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/cape-girardeau-co-sheriffs-dept.json
- cape-girardeau-police-dept: https://data.vsr.recoveredfactory.net/agency_year/cape-girardeau-police-dept.json
- cardwell-police-dept: https://data.vsr.recoveredfactory.net/agency_year/cardwell-police-dept.json
- carl-junction-police-dept: https://data.vsr.recoveredfactory.net/agency_year/carl-junction-police-dept.json
- carroll-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/carroll-county-sheriffs-dept.json
- carrollton-police-dept: https://data.vsr.recoveredfactory.net/agency_year/carrollton-police-dept.json
- carter-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/carter-county-sheriffs-dept.json
- carterville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/carterville-police-dept.json
- carthage-police-dept: https://data.vsr.recoveredfactory.net/agency_year/carthage-police-dept.json
- caruthersville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/caruthersville-police-dept.json
- cass-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/cass-county-sheriffs-dept.json
- cassville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/cassville-police-dept.json
- cedar-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/cedar-county-sheriffs-dept.json
- center-police-dept: https://data.vsr.recoveredfactory.net/agency_year/center-police-dept.json
- centralia-police-dept: https://data.vsr.recoveredfactory.net/agency_year/centralia-police-dept.json
- chaffee-police-dept: https://data.vsr.recoveredfactory.net/agency_year/chaffee-police-dept.json
- chariton-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/chariton-county-sheriffs-dept.json
- charleston-police-dept: https://data.vsr.recoveredfactory.net/agency_year/charleston-police-dept.json
- chesterfield-police-dept: https://data.vsr.recoveredfactory.net/agency_year/chesterfield-police-dept.json
- chillicothe-police-dept: https://data.vsr.recoveredfactory.net/agency_year/chillicothe-police-dept.json
- christian-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/christian-county-sheriffs-dept.json
- city-of-bellerive-acres: https://data.vsr.recoveredfactory.net/agency_year/city-of-bellerive-acres.json
- city-of-bellerive-acres-police-dept: https://data.vsr.recoveredfactory.net/agency_year/city-of-bellerive-acres-police-dept.json
- city-of-california-police-dept: https://data.vsr.recoveredfactory.net/agency_year/city-of-california-police-dept.json
- city-of-oakland-police-dept: https://data.vsr.recoveredfactory.net/agency_year/city-of-oakland-police-dept.json
- clarence-police-dept: https://data.vsr.recoveredfactory.net/agency_year/clarence-police-dept.json
- clark-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/clark-county-sheriffs-dept.json
- clark-police-dept: https://data.vsr.recoveredfactory.net/agency_year/clark-police-dept.json
- clarkton-police-dept: https://data.vsr.recoveredfactory.net/agency_year/clarkton-police-dept.json
- clay-county-parks-dept: https://data.vsr.recoveredfactory.net/agency_year/clay-county-parks-dept.json
- clay-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/clay-county-sheriffs-dept.json
- claycomo-police-dept: https://data.vsr.recoveredfactory.net/agency_year/claycomo-police-dept.json
- clayton-police-dept: https://data.vsr.recoveredfactory.net/agency_year/clayton-police-dept.json
- cleveland-police-dept: https://data.vsr.recoveredfactory.net/agency_year/cleveland-police-dept.json
- clever-police-dept: https://data.vsr.recoveredfactory.net/agency_year/clever-police-dept.json
- clinton-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/clinton-county-sheriffs-dept.json
- clinton-police-dept: https://data.vsr.recoveredfactory.net/agency_year/clinton-police-dept.json
- cole-camp-police-dept: https://data.vsr.recoveredfactory.net/agency_year/cole-camp-police-dept.json
- cole-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/cole-county-sheriffs-dept.json
- columbia-police-dept: https://data.vsr.recoveredfactory.net/agency_year/columbia-police-dept.json
- concordia-police-dept: https://data.vsr.recoveredfactory.net/agency_year/concordia-police-dept.json
- cool-valley-police-dept: https://data.vsr.recoveredfactory.net/agency_year/cool-valley-police-dept.json
- cooper-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/cooper-county-sheriffs-dept.json
- corder-police-dept: https://data.vsr.recoveredfactory.net/agency_year/corder-police-dept.json
- cottleville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/cottleville-police-dept.json
- country-club-hills-police-dept: https://data.vsr.recoveredfactory.net/agency_year/country-club-hills-police-dept.json
- country-club-village-police-dept: https://data.vsr.recoveredfactory.net/agency_year/country-club-village-police-dept.json
- crane-police-dept: https://data.vsr.recoveredfactory.net/agency_year/crane-police-dept.json
- crawford-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/crawford-county-sheriffs-dept.json
- crestwood-police-dept: https://data.vsr.recoveredfactory.net/agency_year/crestwood-police-dept.json
- creve-coeur-police-dept: https://data.vsr.recoveredfactory.net/agency_year/creve-coeur-police-dept.json
- crocker-police-dept: https://data.vsr.recoveredfactory.net/agency_year/crocker-police-dept.json
- crystal-city-police-dept: https://data.vsr.recoveredfactory.net/agency_year/crystal-city-police-dept.json
- crystal-lakes-police-dept: https://data.vsr.recoveredfactory.net/agency_year/crystal-lakes-police-dept.json
- cuba-police-dept: https://data.vsr.recoveredfactory.net/agency_year/cuba-police-dept.json
- dade-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/dade-county-sheriffs-dept.json
- dallas-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/dallas-county-sheriffs-dept.json
- dardenne-prairie-police-dept: https://data.vsr.recoveredfactory.net/agency_year/dardenne-prairie-police-dept.json
- daviess-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/daviess-county-sheriffs-dept.json
- dekalb-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/dekalb-county-sheriffs-dept.json
- dellwood-police-dept: https://data.vsr.recoveredfactory.net/agency_year/dellwood-police-dept.json
- delta-police-dept: https://data.vsr.recoveredfactory.net/agency_year/delta-police-dept.json
- dent-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/dent-county-sheriffs-dept.json
- des-peres-police-dept: https://data.vsr.recoveredfactory.net/agency_year/des-peres-police-dept.json
- desloge-police-dept: https://data.vsr.recoveredfactory.net/agency_year/desloge-police-dept.json
- desoto-police-dept: https://data.vsr.recoveredfactory.net/agency_year/desoto-police-dept.json
- dexter-police-dept: https://data.vsr.recoveredfactory.net/agency_year/dexter-police-dept.json
- diamond-police-dept: https://data.vsr.recoveredfactory.net/agency_year/diamond-police-dept.json
- dixon-police-dept: https://data.vsr.recoveredfactory.net/agency_year/dixon-police-dept.json
- doniphan-police-dept: https://data.vsr.recoveredfactory.net/agency_year/doniphan-police-dept.json
- doolittle-police-dept: https://data.vsr.recoveredfactory.net/agency_year/doolittle-police-dept.json
- douglas-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/douglas-county-sheriffs-dept.json
- drexel-police-dept: https://data.vsr.recoveredfactory.net/agency_year/drexel-police-dept.json
- duenweg-police-dept: https://data.vsr.recoveredfactory.net/agency_year/duenweg-police-dept.json
- dunklin-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/dunklin-county-sheriffs-dept.json
- duquesne-police-dept: https://data.vsr.recoveredfactory.net/agency_year/duquesne-police-dept.json
- east-lynne-police-dept: https://data.vsr.recoveredfactory.net/agency_year/east-lynne-police-dept.json
- east-prairie-police-dept: https://data.vsr.recoveredfactory.net/agency_year/east-prairie-police-dept.json
- easton-police-dept: https://data.vsr.recoveredfactory.net/agency_year/easton-police-dept.json
- edgar-springs-police-dept: https://data.vsr.recoveredfactory.net/agency_year/edgar-springs-police-dept.json
- edgerton-police-dept: https://data.vsr.recoveredfactory.net/agency_year/edgerton-police-dept.json
- edina-police-dept: https://data.vsr.recoveredfactory.net/agency_year/edina-police-dept.json
- edmundson-police-dept: https://data.vsr.recoveredfactory.net/agency_year/edmundson-police-dept.json
- el-dorado-springs-police-dept: https://data.vsr.recoveredfactory.net/agency_year/el-dorado-springs-police-dept.json
- eldon-police-dept: https://data.vsr.recoveredfactory.net/agency_year/eldon-police-dept.json
- ellisville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/ellisville-police-dept.json
- ellsinore-police-dept: https://data.vsr.recoveredfactory.net/agency_year/ellsinore-police-dept.json
- elsberry-police-dept: https://data.vsr.recoveredfactory.net/agency_year/elsberry-police-dept.json
- emma-police-dept: https://data.vsr.recoveredfactory.net/agency_year/emma-police-dept.json
- eureka-police-dept: https://data.vsr.recoveredfactory.net/agency_year/eureka-police-dept.json
- excelsior-springs-police-dept: https://data.vsr.recoveredfactory.net/agency_year/excelsior-springs-police-dept.json
- exeter-police-dept: https://data.vsr.recoveredfactory.net/agency_year/exeter-police-dept.json
- fair-grove-police-dept: https://data.vsr.recoveredfactory.net/agency_year/fair-grove-police-dept.json
- fair-play-police-dept: https://data.vsr.recoveredfactory.net/agency_year/fair-play-police-dept.json
- fairview-police-dept: https://data.vsr.recoveredfactory.net/agency_year/fairview-police-dept.json
- farber-police-dept: https://data.vsr.recoveredfactory.net/agency_year/farber-police-dept.json
- farmington-police-dept: https://data.vsr.recoveredfactory.net/agency_year/farmington-police-dept.json
- fayette-police-dept: https://data.vsr.recoveredfactory.net/agency_year/fayette-police-dept.json
- ferguson-police-dept: https://data.vsr.recoveredfactory.net/agency_year/ferguson-police-dept.json
- ferrelview-police-dept: https://data.vsr.recoveredfactory.net/agency_year/ferrelview-police-dept.json
- festus-police-dept: https://data.vsr.recoveredfactory.net/agency_year/festus-police-dept.json
- fleming-police-dept: https://data.vsr.recoveredfactory.net/agency_year/fleming-police-dept.json
- flordell-hills-police-dept: https://data.vsr.recoveredfactory.net/agency_year/flordell-hills-police-dept.json
- florissant-police-dept: https://data.vsr.recoveredfactory.net/agency_year/florissant-police-dept.json
- foley-police-dept: https://data.vsr.recoveredfactory.net/agency_year/foley-police-dept.json
- fordland-police-dept: https://data.vsr.recoveredfactory.net/agency_year/fordland-police-dept.json
- foristell-police-dept: https://data.vsr.recoveredfactory.net/agency_year/foristell-police-dept.json
- forsyth-police-dept: https://data.vsr.recoveredfactory.net/agency_year/forsyth-police-dept.json
- frankford-police-dept: https://data.vsr.recoveredfactory.net/agency_year/frankford-police-dept.json
- franklin-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/franklin-county-sheriffs-dept.json
- fredericktown-police-dept: https://data.vsr.recoveredfactory.net/agency_year/fredericktown-police-dept.json
- frontenac-police-dept: https://data.vsr.recoveredfactory.net/agency_year/frontenac-police-dept.json
- fulton-police-dept: https://data.vsr.recoveredfactory.net/agency_year/fulton-police-dept.json
- galena-police-dept: https://data.vsr.recoveredfactory.net/agency_year/galena-police-dept.json
- gallatin-police-dept: https://data.vsr.recoveredfactory.net/agency_year/gallatin-police-dept.json
- garden-city-police-dept: https://data.vsr.recoveredfactory.net/agency_year/garden-city-police-dept.json
- gasconade-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/gasconade-county-sheriffs-dept.json
- gasconade-police-dept: https://data.vsr.recoveredfactory.net/agency_year/gasconade-police-dept.json
- gentry-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/gentry-county-sheriffs-dept.json
- gerald-police-dept: https://data.vsr.recoveredfactory.net/agency_year/gerald-police-dept.json
- gideon-police-dept: https://data.vsr.recoveredfactory.net/agency_year/gideon-police-dept.json
- gladstone-dept-of-public-safety: https://data.vsr.recoveredfactory.net/agency_year/gladstone-dept-of-public-safety.json
- glasgow-police-dept: https://data.vsr.recoveredfactory.net/agency_year/glasgow-police-dept.json
- glen-echo-park-police-dept: https://data.vsr.recoveredfactory.net/agency_year/glen-echo-park-police-dept.json
- glendale-police-dept: https://data.vsr.recoveredfactory.net/agency_year/glendale-police-dept.json
- goodman-police-dept: https://data.vsr.recoveredfactory.net/agency_year/goodman-police-dept.json
- gower-police-dept: https://data.vsr.recoveredfactory.net/agency_year/gower-police-dept.json
- grain-valley-police-dept: https://data.vsr.recoveredfactory.net/agency_year/grain-valley-police-dept.json
- granby-police-dept: https://data.vsr.recoveredfactory.net/agency_year/granby-police-dept.json
- grandview-police-dept: https://data.vsr.recoveredfactory.net/agency_year/grandview-police-dept.json
- greene-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/greene-county-sheriffs-dept.json
- greenfield-police-dept: https://data.vsr.recoveredfactory.net/agency_year/greenfield-police-dept.json
- greenwood-police-dept: https://data.vsr.recoveredfactory.net/agency_year/greenwood-police-dept.json
- grundy-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/grundy-county-sheriffs-dept.json
- hallsville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/hallsville-police-dept.json
- hamilton-police-dept: https://data.vsr.recoveredfactory.net/agency_year/hamilton-police-dept.json
- hannibal-police-dept: https://data.vsr.recoveredfactory.net/agency_year/hannibal-police-dept.json
- hardin-police-dept: https://data.vsr.recoveredfactory.net/agency_year/hardin-police-dept.json
- harrison-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/harrison-county-sheriffs-dept.json
- harrisonville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/harrisonville-police-dept.json
- hartville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/hartville-police-dept.json
- hawk-point-police-dept: https://data.vsr.recoveredfactory.net/agency_year/hawk-point-police-dept.json
- hayti-police-dept: https://data.vsr.recoveredfactory.net/agency_year/hayti-police-dept.json
- hazelwood-police-dept: https://data.vsr.recoveredfactory.net/agency_year/hazelwood-police-dept.json
- henrietta-police-dept: https://data.vsr.recoveredfactory.net/agency_year/henrietta-police-dept.json
- henry-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/henry-county-sheriffs-dept.json
- herculaneum-police-dept: https://data.vsr.recoveredfactory.net/agency_year/herculaneum-police-dept.json
- hermann-police-dept: https://data.vsr.recoveredfactory.net/agency_year/hermann-police-dept.json
- hickory-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/hickory-county-sheriffs-dept.json
- higginsville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/higginsville-police-dept.json
- high-hill-police-dept: https://data.vsr.recoveredfactory.net/agency_year/high-hill-police-dept.json
- highlandville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/highlandville-police-dept.json
- hillsboro-police-dept: https://data.vsr.recoveredfactory.net/agency_year/hillsboro-police-dept.json
- hillsdale-police-dept: https://data.vsr.recoveredfactory.net/agency_year/hillsdale-police-dept.json
- holcomb-police-dept: https://data.vsr.recoveredfactory.net/agency_year/holcomb-police-dept.json
- holden-police-dept: https://data.vsr.recoveredfactory.net/agency_year/holden-police-dept.json
- hollister-police-dept: https://data.vsr.recoveredfactory.net/agency_year/hollister-police-dept.json
- holt-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/holt-county-sheriffs-dept.json
- holts-summit-police-dept: https://data.vsr.recoveredfactory.net/agency_year/holts-summit-police-dept.json
- hornersville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/hornersville-police-dept.json
- houston-police-dept: https://data.vsr.recoveredfactory.net/agency_year/houston-police-dept.json
- howard-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/howard-county-sheriffs-dept.json
- howardville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/howardville-police-dept.json
- howell-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/howell-county-sheriffs-dept.json
- humansville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/humansville-police-dept.json
- huntsville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/huntsville-police-dept.json
- iberia-police-dept: https://data.vsr.recoveredfactory.net/agency_year/iberia-police-dept.json
- independence-police-dept: https://data.vsr.recoveredfactory.net/agency_year/independence-police-dept.json
- indian-point-police-dept: https://data.vsr.recoveredfactory.net/agency_year/indian-point-police-dept.json
- iron-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/iron-county-sheriffs-dept.json
- iron-mountain-lake-police-dept: https://data.vsr.recoveredfactory.net/agency_year/iron-mountain-lake-police-dept.json
- ironton-police-dept: https://data.vsr.recoveredfactory.net/agency_year/ironton-police-dept.json
- jackson-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/jackson-county-sheriffs-dept.json
- jackson-police-dept: https://data.vsr.recoveredfactory.net/agency_year/jackson-police-dept.json
- jamestown-police-dept: https://data.vsr.recoveredfactory.net/agency_year/jamestown-police-dept.json
- jasper-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/jasper-county-sheriffs-dept.json
- jasper-police-dept: https://data.vsr.recoveredfactory.net/agency_year/jasper-police-dept.json
- jefferson-city-police-dept: https://data.vsr.recoveredfactory.net/agency_year/jefferson-city-police-dept.json
- jefferson-college-police-dept: https://data.vsr.recoveredfactory.net/agency_year/jefferson-college-police-dept.json
- jefferson-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/jefferson-county-sheriffs-dept.json
- johnson-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/johnson-county-sheriffs-dept.json
- jonesburg-police-dept: https://data.vsr.recoveredfactory.net/agency_year/jonesburg-police-dept.json
- joplin-police-dept: https://data.vsr.recoveredfactory.net/agency_year/joplin-police-dept.json
- kahoka-police-dept: https://data.vsr.recoveredfactory.net/agency_year/kahoka-police-dept.json
- kansas-city-intl-airport-police: https://data.vsr.recoveredfactory.net/agency_year/kansas-city-intl-airport-police.json
- kansas-city-police-dept: https://data.vsr.recoveredfactory.net/agency_year/kansas-city-police-dept.json
- kearney-police-dept: https://data.vsr.recoveredfactory.net/agency_year/kearney-police-dept.json
- kelso-police-dept: https://data.vsr.recoveredfactory.net/agency_year/kelso-police-dept.json
- kennett-police-dept: https://data.vsr.recoveredfactory.net/agency_year/kennett-police-dept.json
- kimberling-city-police-dept: https://data.vsr.recoveredfactory.net/agency_year/kimberling-city-police-dept.json
- kimmswick-police-dept: https://data.vsr.recoveredfactory.net/agency_year/kimmswick-police-dept.json
- kimmwsick-police-dept: https://data.vsr.recoveredfactory.net/agency_year/kimmwsick-police-dept.json
- kingsville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/kingsville-police-dept.json
- kirksville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/kirksville-police-dept.json
- kirkwood-police-dept: https://data.vsr.recoveredfactory.net/agency_year/kirkwood-police-dept.json
- knob-noster-police-dept: https://data.vsr.recoveredfactory.net/agency_year/knob-noster-police-dept.json
- knox-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/knox-county-sheriffs-dept.json
- la-grange-police-dept: https://data.vsr.recoveredfactory.net/agency_year/la-grange-police-dept.json
- laclede-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/laclede-county-sheriffs-dept.json
- laddonia-police-dept: https://data.vsr.recoveredfactory.net/agency_year/laddonia-police-dept.json
- ladue-police-dept: https://data.vsr.recoveredfactory.net/agency_year/ladue-police-dept.json
- lafayette-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/lafayette-county-sheriffs-dept.json
- lake-lafayette-police-dept: https://data.vsr.recoveredfactory.net/agency_year/lake-lafayette-police-dept.json
- lake-lotawana-police-dept: https://data.vsr.recoveredfactory.net/agency_year/lake-lotawana-police-dept.json
- lake-ozark-police-dept: https://data.vsr.recoveredfactory.net/agency_year/lake-ozark-police-dept.json
- lake-st-louis-police-dept: https://data.vsr.recoveredfactory.net/agency_year/lake-st-louis-police-dept.json
- lake-tapawingo-police-dept: https://data.vsr.recoveredfactory.net/agency_year/lake-tapawingo-police-dept.json
- lake-winnebago-police-dept: https://data.vsr.recoveredfactory.net/agency_year/lake-winnebago-police-dept.json
- lakeshire-police-dept: https://data.vsr.recoveredfactory.net/agency_year/lakeshire-police-dept.json
- lamar-police-dept: https://data.vsr.recoveredfactory.net/agency_year/lamar-police-dept.json
- lambert-airport-police-dept: https://data.vsr.recoveredfactory.net/agency_year/lambert-airport-police-dept.json
- lanagan-police-dept: https://data.vsr.recoveredfactory.net/agency_year/lanagan-police-dept.json
- lancaster-police-dept: https://data.vsr.recoveredfactory.net/agency_year/lancaster-police-dept.json
- laplata-police-dept: https://data.vsr.recoveredfactory.net/agency_year/laplata-police-dept.json
- lathrop-police-dept: https://data.vsr.recoveredfactory.net/agency_year/lathrop-police-dept.json
- laurie-police-dept: https://data.vsr.recoveredfactory.net/agency_year/laurie-police-dept.json
- lawrence-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/lawrence-county-sheriffs-dept.json
- lawson-police-dept: https://data.vsr.recoveredfactory.net/agency_year/lawson-police-dept.json
- leadington-police-dept: https://data.vsr.recoveredfactory.net/agency_year/leadington-police-dept.json
- leadwood-police-dept: https://data.vsr.recoveredfactory.net/agency_year/leadwood-police-dept.json
- lebanon-police-dept: https://data.vsr.recoveredfactory.net/agency_year/lebanon-police-dept.json
- lees-summit-police-dept: https://data.vsr.recoveredfactory.net/agency_year/lees-summit-police-dept.json
- leeton-police-dept: https://data.vsr.recoveredfactory.net/agency_year/leeton-police-dept.json
- lewis-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/lewis-county-sheriffs-dept.json
- lexington-police-dept: https://data.vsr.recoveredfactory.net/agency_year/lexington-police-dept.json
- liberal-police-dept: https://data.vsr.recoveredfactory.net/agency_year/liberal-police-dept.json
- liberty-police-dept: https://data.vsr.recoveredfactory.net/agency_year/liberty-police-dept.json
- licking-police-dept: https://data.vsr.recoveredfactory.net/agency_year/licking-police-dept.json
- lincoln-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/lincoln-county-sheriffs-dept.json
- lincoln-police-dept: https://data.vsr.recoveredfactory.net/agency_year/lincoln-police-dept.json
- lincoln-university-police-dept: https://data.vsr.recoveredfactory.net/agency_year/lincoln-university-police-dept.json
- linn-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/linn-county-sheriffs-dept.json
- linn-creek-police-dept: https://data.vsr.recoveredfactory.net/agency_year/linn-creek-police-dept.json
- linn-police-dept: https://data.vsr.recoveredfactory.net/agency_year/linn-police-dept.json
- livingston-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/livingston-county-sheriffs-dept.json
- logan-rogersville-school-police-dept: https://data.vsr.recoveredfactory.net/agency_year/logan-rogersville-school-police-dept.json
- lone-jack-police-dept: https://data.vsr.recoveredfactory.net/agency_year/lone-jack-police-dept.json
- louisiana-police-dept: https://data.vsr.recoveredfactory.net/agency_year/louisiana-police-dept.json
- lowry-city-police-dept: https://data.vsr.recoveredfactory.net/agency_year/lowry-city-police-dept.json
- macon-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/macon-county-sheriffs-dept.json
- macon-police-dept: https://data.vsr.recoveredfactory.net/agency_year/macon-police-dept.json
- madison-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/madison-county-sheriffs-dept.json
- malden-police-dept: https://data.vsr.recoveredfactory.net/agency_year/malden-police-dept.json
- manchester-police-dept: https://data.vsr.recoveredfactory.net/agency_year/manchester-police-dept.json
- mansfield-police-dept: https://data.vsr.recoveredfactory.net/agency_year/mansfield-police-dept.json
- maplewood-police-dept: https://data.vsr.recoveredfactory.net/agency_year/maplewood-police-dept.json
- marble-hill-police-dept: https://data.vsr.recoveredfactory.net/agency_year/marble-hill-police-dept.json
- marceline-police-dept: https://data.vsr.recoveredfactory.net/agency_year/marceline-police-dept.json
- maries-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/maries-county-sheriffs-dept.json
- marion-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/marion-county-sheriffs-dept.json
- marionville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/marionville-police-dept.json
- marquand-police-dept: https://data.vsr.recoveredfactory.net/agency_year/marquand-police-dept.json
- marshall-police-dept: https://data.vsr.recoveredfactory.net/agency_year/marshall-police-dept.json
- marshfield-police-dept: https://data.vsr.recoveredfactory.net/agency_year/marshfield-police-dept.json
- marston-police-dept: https://data.vsr.recoveredfactory.net/agency_year/marston-police-dept.json
- martinsburg-police-dept: https://data.vsr.recoveredfactory.net/agency_year/martinsburg-police-dept.json
- maryland-heights-police-dept: https://data.vsr.recoveredfactory.net/agency_year/maryland-heights-police-dept.json
- maryville-dept-of-public-safety: https://data.vsr.recoveredfactory.net/agency_year/maryville-dept-of-public-safety.json
- matthews-police-dept: https://data.vsr.recoveredfactory.net/agency_year/matthews-police-dept.json
- maysville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/maysville-police-dept.json
- mcdonald-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/mcdonald-county-sheriffs-dept.json
- memphis-police-dept: https://data.vsr.recoveredfactory.net/agency_year/memphis-police-dept.json
- mercer-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/mercer-county-sheriffs-dept.json
- merriam-woods-police-dept: https://data.vsr.recoveredfactory.net/agency_year/merriam-woods-police-dept.json
- metropolitan-community-college-police-dept: https://data.vsr.recoveredfactory.net/agency_year/metropolitan-community-college-police-dept.json
- mexico-police-dept: https://data.vsr.recoveredfactory.net/agency_year/mexico-police-dept.json
- milan-police-dept: https://data.vsr.recoveredfactory.net/agency_year/milan-police-dept.json
- miller-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/miller-county-sheriffs-dept.json
- miner-police-dept: https://data.vsr.recoveredfactory.net/agency_year/miner-police-dept.json
- mississippi-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/mississippi-county-sheriffs-dept.json
- missouri-capitol-police: https://data.vsr.recoveredfactory.net/agency_year/missouri-capitol-police.json
- missouri-dept-natural-resources-park-rangers: https://data.vsr.recoveredfactory.net/agency_year/missouri-dept-natural-resources-park-rangers.json
- missouri-dept-of-conservation-protection-branch: https://data.vsr.recoveredfactory.net/agency_year/missouri-dept-of-conservation-protection-branch.json
- missouri-southern-state-university: https://data.vsr.recoveredfactory.net/agency_year/missouri-southern-state-university.json
- missouri-state-highway-patrol: https://data.vsr.recoveredfactory.net/agency_year/missouri-state-highway-patrol.json
- missouri-univ-sandt-police-dept: https://data.vsr.recoveredfactory.net/agency_year/missouri-univ-sandt-police-dept.json
- missouri-western-state-university-police-dept: https://data.vsr.recoveredfactory.net/agency_year/missouri-western-state-university-police-dept.json
- moberly-police-dept: https://data.vsr.recoveredfactory.net/agency_year/moberly-police-dept.json
- moline-acres-police-dept: https://data.vsr.recoveredfactory.net/agency_year/moline-acres-police-dept.json
- monett-police-dept: https://data.vsr.recoveredfactory.net/agency_year/monett-police-dept.json
- moniteau-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/moniteau-county-sheriffs-dept.json
- monroe-city-police-dept: https://data.vsr.recoveredfactory.net/agency_year/monroe-city-police-dept.json
- monroe-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/monroe-county-sheriffs-dept.json
- montgomery-city-police-dept: https://data.vsr.recoveredfactory.net/agency_year/montgomery-city-police-dept.json
- montgomery-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/montgomery-county-sheriffs-dept.json
- morehouse-police-dept: https://data.vsr.recoveredfactory.net/agency_year/morehouse-police-dept.json
- morgan-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/morgan-county-sheriffs-dept.json
- morley-police-dept: https://data.vsr.recoveredfactory.net/agency_year/morley-police-dept.json
- morrisville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/morrisville-police-dept.json
- moscow-mills-police-dept: https://data.vsr.recoveredfactory.net/agency_year/moscow-mills-police-dept.json
- mound-city-police-dept: https://data.vsr.recoveredfactory.net/agency_year/mound-city-police-dept.json
- mount-vernon-police-dept: https://data.vsr.recoveredfactory.net/agency_year/mount-vernon-police-dept.json
- mountain-grove-police-dept: https://data.vsr.recoveredfactory.net/agency_year/mountain-grove-police-dept.json
- mountain-view-police-dept: https://data.vsr.recoveredfactory.net/agency_year/mountain-view-police-dept.json
- napoleon-police-dept: https://data.vsr.recoveredfactory.net/agency_year/napoleon-police-dept.json
- neosho-police-dept: https://data.vsr.recoveredfactory.net/agency_year/neosho-police-dept.json
- nevada-police-dept: https://data.vsr.recoveredfactory.net/agency_year/nevada-police-dept.json
- new-bloomfield-police-dept: https://data.vsr.recoveredfactory.net/agency_year/new-bloomfield-police-dept.json
- new-florence-police-dept: https://data.vsr.recoveredfactory.net/agency_year/new-florence-police-dept.json
- new-haven-police-dept: https://data.vsr.recoveredfactory.net/agency_year/new-haven-police-dept.json
- new-london-police-dept: https://data.vsr.recoveredfactory.net/agency_year/new-london-police-dept.json
- new-madrid-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/new-madrid-county-sheriffs-dept.json
- new-madrid-police-dept: https://data.vsr.recoveredfactory.net/agency_year/new-madrid-police-dept.json
- newburg-police-dept: https://data.vsr.recoveredfactory.net/agency_year/newburg-police-dept.json
- newton-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/newton-county-sheriffs-dept.json
- niangua-police-dept: https://data.vsr.recoveredfactory.net/agency_year/niangua-police-dept.json
- nixa-police-dept: https://data.vsr.recoveredfactory.net/agency_year/nixa-police-dept.json
- nixa-schools-police-dept: https://data.vsr.recoveredfactory.net/agency_year/nixa-schools-police-dept.json
- nodaway-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/nodaway-county-sheriffs-dept.json
- noel-police-dept: https://data.vsr.recoveredfactory.net/agency_year/noel-police-dept.json
- norfolk-southern-railway-police-dept: https://data.vsr.recoveredfactory.net/agency_year/norfolk-southern-railway-police-dept.json
- normandy-police-dept: https://data.vsr.recoveredfactory.net/agency_year/normandy-police-dept.json
- north-county-police-cooperative: https://data.vsr.recoveredfactory.net/agency_year/north-county-police-cooperative.json
- north-kansas-city-police-dept: https://data.vsr.recoveredfactory.net/agency_year/north-kansas-city-police-dept.json
- northmoor-police-dept: https://data.vsr.recoveredfactory.net/agency_year/northmoor-police-dept.json
- northwest-mo-university-police-dept: https://data.vsr.recoveredfactory.net/agency_year/northwest-mo-university-police-dept.json
- northwoods-police-dept: https://data.vsr.recoveredfactory.net/agency_year/northwoods-police-dept.json
- nw-mo-state-university-police-dept: https://data.vsr.recoveredfactory.net/agency_year/nw-mo-state-university-police-dept.json
- oak-grove-police-dept: https://data.vsr.recoveredfactory.net/agency_year/oak-grove-police-dept.json
- oakview-police-dept: https://data.vsr.recoveredfactory.net/agency_year/oakview-police-dept.json
- odessa-police-dept: https://data.vsr.recoveredfactory.net/agency_year/odessa-police-dept.json
- ofallon-police-dept: https://data.vsr.recoveredfactory.net/agency_year/ofallon-police-dept.json
- old-monroe-police-dept: https://data.vsr.recoveredfactory.net/agency_year/old-monroe-police-dept.json
- olivette-police-dept: https://data.vsr.recoveredfactory.net/agency_year/olivette-police-dept.json
- oran-police-dept: https://data.vsr.recoveredfactory.net/agency_year/oran-police-dept.json
- oregon-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/oregon-county-sheriffs-dept.json
- oregon-police-dept: https://data.vsr.recoveredfactory.net/agency_year/oregon-police-dept.json
- oronogo-police-dept: https://data.vsr.recoveredfactory.net/agency_year/oronogo-police-dept.json
- orrick-police-dept: https://data.vsr.recoveredfactory.net/agency_year/orrick-police-dept.json
- osage-beach-police-dept: https://data.vsr.recoveredfactory.net/agency_year/osage-beach-police-dept.json
- osage-co-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/osage-co-sheriffs-dept.json
- osage-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/osage-county-sheriffs-dept.json
- osceola-police-dept: https://data.vsr.recoveredfactory.net/agency_year/osceola-police-dept.json
- otterville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/otterville-police-dept.json
- overland-police-dept: https://data.vsr.recoveredfactory.net/agency_year/overland-police-dept.json
- owensville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/owensville-police-dept.json
- ozark-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/ozark-county-sheriffs-dept.json
- ozark-police-dept: https://data.vsr.recoveredfactory.net/agency_year/ozark-police-dept.json
- pacific-police-dept: https://data.vsr.recoveredfactory.net/agency_year/pacific-police-dept.json
- pagedale-police-dept: https://data.vsr.recoveredfactory.net/agency_year/pagedale-police-dept.json
- palmyra-police-dept: https://data.vsr.recoveredfactory.net/agency_year/palmyra-police-dept.json
- park-hills-police-dept: https://data.vsr.recoveredfactory.net/agency_year/park-hills-police-dept.json
- parkville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/parkville-police-dept.json
- parma-police-dept: https://data.vsr.recoveredfactory.net/agency_year/parma-police-dept.json
- pasadena-park-police-dept: https://data.vsr.recoveredfactory.net/agency_year/pasadena-park-police-dept.json
- peculiar-police-dept: https://data.vsr.recoveredfactory.net/agency_year/peculiar-police-dept.json
- pemiscot-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/pemiscot-county-sheriffs-dept.json
- perry-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/perry-county-sheriffs-dept.json
- perry-police-dept: https://data.vsr.recoveredfactory.net/agency_year/perry-police-dept.json
- perryville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/perryville-police-dept.json
- pettis-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/pettis-county-sheriffs-dept.json
- pevely-police-dept: https://data.vsr.recoveredfactory.net/agency_year/pevely-police-dept.json
- phelps-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/phelps-county-sheriffs-dept.json
- piedmont-police-dept: https://data.vsr.recoveredfactory.net/agency_year/piedmont-police-dept.json
- pierce-city-police-dept: https://data.vsr.recoveredfactory.net/agency_year/pierce-city-police-dept.json
- pike-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/pike-county-sheriffs-dept.json
- pilot-grove-police-dept: https://data.vsr.recoveredfactory.net/agency_year/pilot-grove-police-dept.json
- pilot-knob-police-dept: https://data.vsr.recoveredfactory.net/agency_year/pilot-knob-police-dept.json
- pine-lawn-police-dept: https://data.vsr.recoveredfactory.net/agency_year/pine-lawn-police-dept.json
- pineville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/pineville-police-dept.json
- platte-city-police-dept: https://data.vsr.recoveredfactory.net/agency_year/platte-city-police-dept.json
- platte-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/platte-county-sheriffs-dept.json
- platte-woods-police-dept: https://data.vsr.recoveredfactory.net/agency_year/platte-woods-police-dept.json
- plattsburg-police-dept: https://data.vsr.recoveredfactory.net/agency_year/plattsburg-police-dept.json
- pleasant-hill-police-dept: https://data.vsr.recoveredfactory.net/agency_year/pleasant-hill-police-dept.json
- pleasant-hope-police-dept: https://data.vsr.recoveredfactory.net/agency_year/pleasant-hope-police-dept.json
- pleasant-valley-police-dept: https://data.vsr.recoveredfactory.net/agency_year/pleasant-valley-police-dept.json
- polk-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/polk-county-sheriffs-dept.json
- polo-police-dept: https://data.vsr.recoveredfactory.net/agency_year/polo-police-dept.json
- poplar-bluff-police-dept: https://data.vsr.recoveredfactory.net/agency_year/poplar-bluff-police-dept.json
- portageville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/portageville-police-dept.json
- potosi-police-dept: https://data.vsr.recoveredfactory.net/agency_year/potosi-police-dept.json
- prairie-home-police-dept: https://data.vsr.recoveredfactory.net/agency_year/prairie-home-police-dept.json
- pulaski-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/pulaski-county-sheriffs-dept.json
- purdy-police-dept: https://data.vsr.recoveredfactory.net/agency_year/purdy-police-dept.json
- putnam-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/putnam-county-sheriffs-dept.json
- puxico-police-dept: https://data.vsr.recoveredfactory.net/agency_year/puxico-police-dept.json
- queen-city-police-dept: https://data.vsr.recoveredfactory.net/agency_year/queen-city-police-dept.json
- qulin-police-dept: https://data.vsr.recoveredfactory.net/agency_year/qulin-police-dept.json
- ralls-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/ralls-county-sheriffs-dept.json
- randolph-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/randolph-county-sheriffs-dept.json
- ray-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/ray-county-sheriffs-dept.json
- raymore-police-dept: https://data.vsr.recoveredfactory.net/agency_year/raymore-police-dept.json
- raytown-police-dept: https://data.vsr.recoveredfactory.net/agency_year/raytown-police-dept.json
- reeds-spring-police-dept: https://data.vsr.recoveredfactory.net/agency_year/reeds-spring-police-dept.json
- republic-police-dept: https://data.vsr.recoveredfactory.net/agency_year/republic-police-dept.json
- reynolds-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/reynolds-county-sheriffs-dept.json
- rich-hill-police-dept: https://data.vsr.recoveredfactory.net/agency_year/rich-hill-police-dept.json
- richland-police-dept: https://data.vsr.recoveredfactory.net/agency_year/richland-police-dept.json
- richmond-heights-police-dept: https://data.vsr.recoveredfactory.net/agency_year/richmond-heights-police-dept.json
- richmond-police-dept: https://data.vsr.recoveredfactory.net/agency_year/richmond-police-dept.json
- ripley-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/ripley-county-sheriffs-dept.json
- riverside-police-dept: https://data.vsr.recoveredfactory.net/agency_year/riverside-police-dept.json
- riverview-police-dept: https://data.vsr.recoveredfactory.net/agency_year/riverview-police-dept.json
- rock-hill-police-dept: https://data.vsr.recoveredfactory.net/agency_year/rock-hill-police-dept.json
- rock-port-police-dept: https://data.vsr.recoveredfactory.net/agency_year/rock-port-police-dept.json
- rockaway-beach-police-dept: https://data.vsr.recoveredfactory.net/agency_year/rockaway-beach-police-dept.json
- rogersville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/rogersville-police-dept.json
- rolla-police-dept: https://data.vsr.recoveredfactory.net/agency_year/rolla-police-dept.json
- rosebud-police-dept: https://data.vsr.recoveredfactory.net/agency_year/rosebud-police-dept.json
- salem-police-dept: https://data.vsr.recoveredfactory.net/agency_year/salem-police-dept.json
- saline-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/saline-county-sheriffs-dept.json
- salisbury-police-dept: https://data.vsr.recoveredfactory.net/agency_year/salisbury-police-dept.json
- sarcoxie-police-dept: https://data.vsr.recoveredfactory.net/agency_year/sarcoxie-police-dept.json
- savannah-police-dept: https://data.vsr.recoveredfactory.net/agency_year/savannah-police-dept.json
- schuyler-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/schuyler-county-sheriffs-dept.json
- scotland-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/scotland-county-sheriffs-dept.json
- scott-city-police-dept: https://data.vsr.recoveredfactory.net/agency_year/scott-city-police-dept.json
- scott-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/scott-county-sheriffs-dept.json
- se-mo-state-university-dept-of-public-safety: https://data.vsr.recoveredfactory.net/agency_year/se-mo-state-university-dept-of-public-safety.json
- sedalia-police-dept: https://data.vsr.recoveredfactory.net/agency_year/sedalia-police-dept.json
- seligman-police-dept: https://data.vsr.recoveredfactory.net/agency_year/seligman-police-dept.json
- senath-police-dept: https://data.vsr.recoveredfactory.net/agency_year/senath-police-dept.json
- seneca-police-dept: https://data.vsr.recoveredfactory.net/agency_year/seneca-police-dept.json
- seymour-police-dept: https://data.vsr.recoveredfactory.net/agency_year/seymour-police-dept.json
- shannon-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/shannon-county-sheriffs-dept.json
- shelbina-police-dept: https://data.vsr.recoveredfactory.net/agency_year/shelbina-police-dept.json
- shelby-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/shelby-county-sheriffs-dept.json
- shrewsbury-police-dept: https://data.vsr.recoveredfactory.net/agency_year/shrewsbury-police-dept.json
- sikeston-police-dept: https://data.vsr.recoveredfactory.net/agency_year/sikeston-police-dept.json
- slater-police-dept: https://data.vsr.recoveredfactory.net/agency_year/slater-police-dept.json
- smithton-police-dept: https://data.vsr.recoveredfactory.net/agency_year/smithton-police-dept.json
- smithville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/smithville-police-dept.json
- southwest-city-police-dept: https://data.vsr.recoveredfactory.net/agency_year/southwest-city-police-dept.json
- sparta-police-dept: https://data.vsr.recoveredfactory.net/agency_year/sparta-police-dept.json
- springfield-branson-national-airport-police: https://data.vsr.recoveredfactory.net/agency_year/springfield-branson-national-airport-police.json
- springfield-greene-county-park-rangers: https://data.vsr.recoveredfactory.net/agency_year/springfield-greene-county-park-rangers.json
- springfield-police-dept: https://data.vsr.recoveredfactory.net/agency_year/springfield-police-dept.json
- springfield-school-police: https://data.vsr.recoveredfactory.net/agency_year/springfield-school-police.json
- st-ann-police-dept: https://data.vsr.recoveredfactory.net/agency_year/st-ann-police-dept.json
- st-charles-city-parks-and-recreation-dept: https://data.vsr.recoveredfactory.net/agency_year/st-charles-city-parks-and-recreation-dept.json
- st-charles-community-college-police-dept: https://data.vsr.recoveredfactory.net/agency_year/st-charles-community-college-police-dept.json
- st-charles-county-police-dept: https://data.vsr.recoveredfactory.net/agency_year/st-charles-county-police-dept.json
- st-charles-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/st-charles-county-sheriffs-dept.json
- st-charles-police-dept: https://data.vsr.recoveredfactory.net/agency_year/st-charles-police-dept.json
- st-clair-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/st-clair-county-sheriffs-dept.json
- st-clair-police-dept: https://data.vsr.recoveredfactory.net/agency_year/st-clair-police-dept.json
- st-francois-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/st-francois-county-sheriffs-dept.json
- st-james-police-dept: https://data.vsr.recoveredfactory.net/agency_year/st-james-police-dept.json
- st-john-police-dept: https://data.vsr.recoveredfactory.net/agency_year/st-john-police-dept.json
- st-joseph-police-dept: https://data.vsr.recoveredfactory.net/agency_year/st-joseph-police-dept.json
- st-louis-city-police-dept: https://data.vsr.recoveredfactory.net/agency_year/st-louis-city-police-dept.json
- st-louis-community-college-police-dept: https://data.vsr.recoveredfactory.net/agency_year/st-louis-community-college-police-dept.json
- st-louis-county-park-rangers: https://data.vsr.recoveredfactory.net/agency_year/st-louis-county-park-rangers.json
- st-louis-county-police-dept: https://data.vsr.recoveredfactory.net/agency_year/st-louis-county-police-dept.json
- st-peters-police-dept: https://data.vsr.recoveredfactory.net/agency_year/st-peters-police-dept.json
- st-robert-police-dept: https://data.vsr.recoveredfactory.net/agency_year/st-robert-police-dept.json
- ste-genevieve-co-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/ste-genevieve-co-sheriffs-dept.json
- ste-genevieve-police-dept: https://data.vsr.recoveredfactory.net/agency_year/ste-genevieve-police-dept.json
- steele-police-dept: https://data.vsr.recoveredfactory.net/agency_year/steele-police-dept.json
- steelville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/steelville-police-dept.json
- stockton-police-dept: https://data.vsr.recoveredfactory.net/agency_year/stockton-police-dept.json
- stoddard-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/stoddard-county-sheriffs-dept.json
- stone-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/stone-county-sheriffs-dept.json
- stover-police-dept: https://data.vsr.recoveredfactory.net/agency_year/stover-police-dept.json
- strafford-police-dept: https://data.vsr.recoveredfactory.net/agency_year/strafford-police-dept.json
- strasburg-police-dept: https://data.vsr.recoveredfactory.net/agency_year/strasburg-police-dept.json
- sturgeon-police-dept: https://data.vsr.recoveredfactory.net/agency_year/sturgeon-police-dept.json
- sugar-creek-police-dept: https://data.vsr.recoveredfactory.net/agency_year/sugar-creek-police-dept.json
- sullivan-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/sullivan-county-sheriffs-dept.json
- sullivan-police-dept: https://data.vsr.recoveredfactory.net/agency_year/sullivan-police-dept.json
- summersville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/summersville-police-dept.json
- sunrise-beach-police-dept: https://data.vsr.recoveredfactory.net/agency_year/sunrise-beach-police-dept.json
- sunset-hills-police-dept: https://data.vsr.recoveredfactory.net/agency_year/sunset-hills-police-dept.json
- sweet-springs-police-dept: https://data.vsr.recoveredfactory.net/agency_year/sweet-springs-police-dept.json
- sycamore-hills-police-dept: https://data.vsr.recoveredfactory.net/agency_year/sycamore-hills-police-dept.json
- taney-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/taney-county-sheriffs-dept.json
- tarkio-police-dept: https://data.vsr.recoveredfactory.net/agency_year/tarkio-police-dept.json
- terre-du-lac-police-dept: https://data.vsr.recoveredfactory.net/agency_year/terre-du-lac-police-dept.json
- texas-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/texas-county-sheriffs-dept.json
- thayer-police-dept: https://data.vsr.recoveredfactory.net/agency_year/thayer-police-dept.json
- tipton-police-dept: https://data.vsr.recoveredfactory.net/agency_year/tipton-police-dept.json
- town-and-country-police-dept: https://data.vsr.recoveredfactory.net/agency_year/town-and-country-police-dept.json
- tracy-police-dept: https://data.vsr.recoveredfactory.net/agency_year/tracy-police-dept.json
- trenton-police-dept: https://data.vsr.recoveredfactory.net/agency_year/trenton-police-dept.json
- troy-police-dept: https://data.vsr.recoveredfactory.net/agency_year/troy-police-dept.json
- truesdale-police-dept: https://data.vsr.recoveredfactory.net/agency_year/truesdale-police-dept.json
- truman-state-univ-dept-of-ps: https://data.vsr.recoveredfactory.net/agency_year/truman-state-univ-dept-of-ps.json
- ucm-dept-of-public-safety: https://data.vsr.recoveredfactory.net/agency_year/ucm-dept-of-public-safety.json
- union-pacific-rr-police-kansas-city-st-louis: https://data.vsr.recoveredfactory.net/agency_year/union-pacific-rr-police-kansas-city-st-louis.json
- union-police-dept: https://data.vsr.recoveredfactory.net/agency_year/union-police-dept.json
- unionville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/unionville-police-dept.json
- university-city-police-dept: https://data.vsr.recoveredfactory.net/agency_year/university-city-police-dept.json
- university-of-missouri-columbia-police-dept: https://data.vsr.recoveredfactory.net/agency_year/university-of-missouri-columbia-police-dept.json
- university-of-missouri-kansas-city-police-dept: https://data.vsr.recoveredfactory.net/agency_year/university-of-missouri-kansas-city-police-dept.json
- university-of-missouri-st-louis-police-dept: https://data.vsr.recoveredfactory.net/agency_year/university-of-missouri-st-louis-police-dept.json
- uplands-park-police-dept: https://data.vsr.recoveredfactory.net/agency_year/uplands-park-police-dept.json
- van-buren-police-dept: https://data.vsr.recoveredfactory.net/agency_year/van-buren-police-dept.json
- velda-city-police-dept: https://data.vsr.recoveredfactory.net/agency_year/velda-city-police-dept.json
- vernon-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/vernon-county-sheriffs-dept.json
- verona-police-dept: https://data.vsr.recoveredfactory.net/agency_year/verona-police-dept.json
- versailles-police-dept: https://data.vsr.recoveredfactory.net/agency_year/versailles-police-dept.json
- viburnum-police-dept: https://data.vsr.recoveredfactory.net/agency_year/viburnum-police-dept.json
- vienna-police-dept: https://data.vsr.recoveredfactory.net/agency_year/vienna-police-dept.json
- vinita-park-police-dept: https://data.vsr.recoveredfactory.net/agency_year/vinita-park-police-dept.json
- walnut-grove-police-dept: https://data.vsr.recoveredfactory.net/agency_year/walnut-grove-police-dept.json
- warren-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/warren-county-sheriffs-dept.json
- warrensburg-police-dept: https://data.vsr.recoveredfactory.net/agency_year/warrensburg-police-dept.json
- warrenton-police-dept: https://data.vsr.recoveredfactory.net/agency_year/warrenton-police-dept.json
- warsaw-police-dept: https://data.vsr.recoveredfactory.net/agency_year/warsaw-police-dept.json
- warson-woods-police-dept: https://data.vsr.recoveredfactory.net/agency_year/warson-woods-police-dept.json
- washburn-police-dept: https://data.vsr.recoveredfactory.net/agency_year/washburn-police-dept.json
- washington-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/washington-county-sheriffs-dept.json
- washington-police-dept: https://data.vsr.recoveredfactory.net/agency_year/washington-police-dept.json
- washington-university-police-dept: https://data.vsr.recoveredfactory.net/agency_year/washington-university-police-dept.json
- waverly-police-dept: https://data.vsr.recoveredfactory.net/agency_year/waverly-police-dept.json
- wayne-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/wayne-county-sheriffs-dept.json
- waynesville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/waynesville-police-dept.json
- weatherby-lake-police-dept: https://data.vsr.recoveredfactory.net/agency_year/weatherby-lake-police-dept.json
- webb-city-police-dept: https://data.vsr.recoveredfactory.net/agency_year/webb-city-police-dept.json
- webster-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/webster-county-sheriffs-dept.json
- webster-groves-police-dept: https://data.vsr.recoveredfactory.net/agency_year/webster-groves-police-dept.json
- weldon-spring-police-dept: https://data.vsr.recoveredfactory.net/agency_year/weldon-spring-police-dept.json
- wellington-police-dept: https://data.vsr.recoveredfactory.net/agency_year/wellington-police-dept.json
- wellston-police-dept: https://data.vsr.recoveredfactory.net/agency_year/wellston-police-dept.json
- wellsville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/wellsville-police-dept.json
- wentzville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/wentzville-police-dept.json
- west-plains-police-dept: https://data.vsr.recoveredfactory.net/agency_year/west-plains-police-dept.json
- weston-police-dept: https://data.vsr.recoveredfactory.net/agency_year/weston-police-dept.json
- wheaton-police-dept: https://data.vsr.recoveredfactory.net/agency_year/wheaton-police-dept.json
- willard-police-dept: https://data.vsr.recoveredfactory.net/agency_year/willard-police-dept.json
- willard-school-police-dept: https://data.vsr.recoveredfactory.net/agency_year/willard-school-police-dept.json
- williamsville-police-dept: https://data.vsr.recoveredfactory.net/agency_year/williamsville-police-dept.json
- willow-springs-police-dept: https://data.vsr.recoveredfactory.net/agency_year/willow-springs-police-dept.json
- winfield-police-dept: https://data.vsr.recoveredfactory.net/agency_year/winfield-police-dept.json
- winona-police-dept: https://data.vsr.recoveredfactory.net/agency_year/winona-police-dept.json
- wood-heights-police-dept: https://data.vsr.recoveredfactory.net/agency_year/wood-heights-police-dept.json
- woodson-terrace-police-dept: https://data.vsr.recoveredfactory.net/agency_year/woodson-terrace-police-dept.json
- worth-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/worth-county-sheriffs-dept.json
- wright-city-police-dept: https://data.vsr.recoveredfactory.net/agency_year/wright-city-police-dept.json
- wright-county-sheriffs-dept: https://data.vsr.recoveredfactory.net/agency_year/wright-county-sheriffs-dept.json
<!-- AGENCY_SLUGS_END -->

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
