# Missouri Vehicle Stops Report Dagster Project

Process data from the [Missouri Vehicle Stops report](https://ago.mo.gov/get-help/vehicle-stops-report/) (VSR). 

This project is a data pipeline implemented with Dagster. It extracts data from PDFs published by the state to create a canonical database in a tidy format. It then joins those records to additional information we have collected: a) A database of contact information for each department, b) a cross walk of agency to county where they have jurisdiction, and c) the agency's comments on their VSR submission.

pulling data from Airtable, processing it with pandas, and uploading results to an S3 bucket. It consists of multiple assets for:

- Fetching and storing Airtable data.
- Stripping personal information for analysis.
- Generating database documentation in Markdown.
- Uploading results to S3 with pre-signed URLs.

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
AIRTABLE_API_KEY=<your-airtable-api-key>
AIRTABLE_BASE_ID=<your-airtable-base-id>
GOOGLE_DRIVE_OUTPUT_FOLDER_ID=<your-output-folder-id>
GOOGLE_APPLICATION_CREDENTIALS=<absolute-path-to-credentials-if-not-project-default>
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
uv run dagster asset materialize --select ASSET_NAME -m deaths_in_custody.definitions
```

Dagster caches materialized assets, but they don't persist between runs of the web UI or CLI tool unless you set the `DAGSTER_HOME` environment variable in your global environment or `.env` file. See the [Dagster docs](https://docs.dagster.io/guides/deploy/dagster-instance-configuration#default-local-behavior) page on local behavior for more information.


## Assets/usage

- **Fetching Airtable data:** The `airtable_data` asset retrieves records and store them as JSON.
- **Fetching clip search PDFs:** The `clip_pdfs` asset retrieves PDFs exported from Lexis Nexis and stored on Google Drive.
- **Processing data:**  The `analysis_data` asset removes personal information and outputs a cleaned dataset.
- **Generating documentation:** The `documentation` asset produces a Markdown summary of the Airtable schema.
- **Uploading to S3:** The `s3_files` asset uploads files in the `out` directory to the configured S3 bucket with pre-signed URLs.
- **Uploading to Google Drive:** The `google_drive_files` asset materializes files in the `out` directory to Google drive. 

## Using the data

There are three main ways you can use the data:

* The local `data` folder includes all the key outputs and you can write local code in this repository that analyzes it or further processes it. The `Notebooks` folder contains at least one Jupyter notebook that demonstrates this idea. Code that performs critical transformations (specifically transformations that benefit from automated checking) should ultimately be moved back into the pipeline.
* A [Google drive output folder](https://drive.google.com/drive/folders/1A4yiR2IepJaSBrv54XHIIIsD-gtIIN0r) is available for people who want to work with flat data in Google Sheets or read the database documentation in a Google Doc.
* S3 web URLs can be generated from the pipeline to generate expiring, presigned URLs for use in networked environments, e.g. an Observable or Colab notebook. These URLs are generated at runtime -- to get them, run the pipeline -- and should be treated like any internal secret.  

## Debugging and Logs

Logs are available in the Dagster UI. 

# Addenda: 🔐 Google Drive Integration (via Service Account)

This project uses a Google **Service Account** to download and upload files to shared Google Drive folders. Files from `data/out` are automatically uploaded to the output folder, while clips from a search are saved in `data/src/clips`.

To recreate, you'll need a Cloud Console project and service account.

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create or select a project
3. Navigate to **IAM & Admin > Service Accounts**
4. Click **Create Service Account**
   - Name it something like `dagster-uploader`
   - Assign the role: `Editor` (or `Drive API > Drive File Creator` for more granular permissions)
5. After creation, go to the **Keys** tab
6. Click **Add Key > Create new key > JSON**
7. Save the JSON file securely — you'll reference this in your environment config

In the [Google API Library](https://console.cloud.google.com/apis/library):

- Enable **Google Drive API**
- Enable **Google Sheets API**

Then, share the drive folder:

1. In Google Drive, open the folder you want the asset to write to
2. Click **Share** and add the service account email (e.g. `dagster-uploader@your-project.iam.gserviceaccount.com`)
3. Set permission to **Editor**