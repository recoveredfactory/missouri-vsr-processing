# Prompt for Codex

Hi there, I've been really struggling with something and want to refactor this codebase to function correctly.

I have a python / dagster pipeline that reads a bunch of PDFs and is meant to extract a bunch of data using Camelot. In true governemnt PDF fashion, the table formatting leaves a lot to be desired and parsing is required. 

The tables are formatted conceptually like so:

              | Total | White | Black | ...
Totals        | .     | .     | .     | ...
  All stops   | 23    | 13    | 9     | ...
  Searches    | 10    | 3     | 7     | ...
Rates         | .     | .     | .     | ...
  Stop rate   | 30.59 | 30.1  | 33.2  | ...
  Arrest rate | .     | .     | .     | ...

Of course, the columns come into camelot misaligned, and we should talk about the structure:

The columns are always either the leftmost "label" value, or are race values with counts or rates.

Each table is broken into sections. So to identify a row, we need to know its section and "metric" -- for example, "Totals" is the section and "All stops" is the metric.

Section rows are always flush with the left edge of the table. Metric rows are always indented. 

Section rows are always followed by a series for dots, one for each column. Metric rows, beguilingly and as my example shows, _can_ be followed by a series of dots for each column, so just looking at that heuristic is not enough to identify the section row.

I want you to look at all the stuff between lines 121 and 427 of missouri_vsr/assets.py and refactor the whole thing to use camelot features and row sniffing to construct an accurate tidy tabular version of this, with section and metric and then the values.

In all cases, there's a department name that comes before the tables, which you'll see in the code and need to preserve. What else do you need to know?
Updates implemented:
- Added indent-based section detection using Camelot cell x-coordinates (flush-left rows become section headers; following indented rows are metrics).
- Normalized key + seven numeric columns via right-to-left numeric sniffing to handle misaligned columns.
- Added toggle to use local example PDFs instead of full reports by setting env var `VSR_USE_EXAMPLES=1`.
- Example mapping: 2024 → `data/src/examples/Example-Alma-VSRreport2024.pdf`.

How to run with examples:
- Export `VSR_USE_EXAMPLES=1` in your env before running Dagster.
- Materialize `download_reports` output for 2024 and the downstream `extract_pdf_data_2024`.
- The pipeline will skip downloads and read the example PDF directly.

# Pages to extract for 2023 fast sample

10-13,114-117,1734-1737,1722-1725,1726-1728

# Run config for s3

resources:
  data_dir_report_pdfs:
      config:
        path: data/src/reports
  data_dir_processed:
      config:
        path: data/processed
  s3:
      config:
        bucket: tmp-gfx-public-data
        s3_prefix: missouri-vsr/
        presigned_expiration: 7776000