# Missouri Vehicle Stops Report Dagster Project

Process data from the [Missouri Vehicle Stops report](https://ago.mo.gov/get-help/vehicle-stops-report/) (VSR). 

This project is a data pipeline implemented with Dagster. It extracts data from PDFs published by the state to create a canonical database in a tidy format. In the future, it will then join those records to additional information we have collected: a) A database of contact information for each department, b) a cross-walk of agency to county where they have jurisdiction, and c) the agency's comments on their VSR submission.

# Project Setup and Execution

## Prerequisites

Ensure you have the following installed:

- Python 3.12+ 
- [uv](https://docs.astral.sh/uv/) for dependency management (`brew install uv`)

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

The project relies on environment variables for Airtable, Google Drive, and S3 configurations. Create a `.env` file in the project root with the following keys and your values:

```ini
MISSOURI_VSR_BUCKET_NAME=<your-s3-bucket>
```

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

You can also materialize these assets via the command line:

```sh
uv run dagster asset materialize --select ASSET_NAME -m missouri_vsr.definitions
```

Dagster caches materialized assets, but they don't persist between runs of the web UI or CLI tool unless you set the `DAGSTER_HOME` environment variable in your global environment or `.env` file. See the [Dagster docs](https://docs.dagster.io/guides/deploy/dagster-instance-configuration#default-local-behavior) page on local behavior for more information.

