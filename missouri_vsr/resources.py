import boto3
import gzip
import io
import mimetypes
import os
import tempfile
import typing
import uuid

from dagster import ConfigurableResource
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from pathlib import Path
from pyairtable import Api
from botocore.config import Config


class AirtableBaseResource(ConfigurableResource):
    """
    A Dagster resource for interacting with an Airtable base.  
    """
    api_key: str
    base_id: str

    @property
    def _api(self):
        return Api(self.api_key)

    @property
    def base(self):
        return self._api.base(self.base_id)


class S3Resource(ConfigurableResource):
    """
    A Dagster resource for interacting with S3.
    
    Configurable via run config with the following keys:
      - bucket: The S3 bucket name.
      - s3_prefix: A key prefix that will be prepended to all uploaded files.
      - presigned_expiration: Expiration (in seconds) for generated pre-signed URLs.
      - temp_dir: (Optional) A temporary directory path; if not provided, the system temp directory is used.
      - region: (Optional) AWS region; falls back to AWS_REGION or AWS_DEFAULT_REGION.
    """
    bucket: typing.Optional[str] = None
    s3_prefix: str = ""
    # Default presigned URL expiration: 45 days.
    presigned_expiration: int = 45 * 24 * 60 * 60
    region: typing.Optional[str] = None
    temp_dir: str = None

    def __post_init__(self):
        # Avoid persisting boto3 clients on the resource instance (pydantic models are frozen).
        if self.temp_dir is None:
            self.temp_dir = tempfile.gettempdir()

    def resolved_bucket(self) -> typing.Optional[str]:
        """Bucket from config or env."""
        return self.bucket or os.getenv("MISSOURI_VSR_BUCKET_NAME") or os.getenv("AWS_S3_BUCKET")

    def resolved_prefix(self) -> str:
        """Prefix from config or env (no leading/trailing slash)."""
        prefix = self.s3_prefix or os.getenv("MISSOURI_VSR_S3_PREFIX") or ""
        return prefix.strip("/")

    def resolved_region(self) -> typing.Optional[str]:
        """Region from config or env."""
        return self.region or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")

    def client(self):
        """S3 client with signature v4 and optional region."""
        cfg = Config(signature_version="s3v4")
        region = self.resolved_region()
        if region:
            return boto3.client("s3", region_name=region, config=cfg)
        return boto3.client("s3", config=cfg)

    def obfuscate_filename(self, filename: str) -> str:
        """Append a short unique identifier to the filename."""
        unique_id = uuid.uuid4().hex[:8]
        return f"{unique_id}_{filename}"

    def upload_file(self, local_path: str, s3_key: str) -> str:
        """
        Uploads a file from `local_path` to S3 under the provided `s3_key`.
        
        If the file is text-based (ends with .json, .csv, or .txt), it is compressed with gzip before uploading.
        Returns a pre-signed URL for the uploaded file.
        """
        bucket = self.resolved_bucket()
        if not bucket:
            print("S3 bucket not configured; skipping upload.")
            return None

        try:
            is_text = local_path.endswith((".json", ".csv", ".txt"))
            if is_text:
                tmp_path = os.path.join(self.temp_dir, os.path.basename(local_path) + ".gz")
                with open(local_path, "rb") as f_in:
                    with gzip.open(tmp_path, "wb") as f_out:
                        # This copies the file content into a gzipped file.
                        f_out.writelines(f_in)
            else:
                tmp_path = local_path

            extra_args = {}
            if is_text:
                extra_args.update({
                    "ContentType": "text/plain",
                    "ContentEncoding": "gzip"
                })

            # Create a client per call to avoid frozen-instance mutation
            s3 = self.client()
            # Upload the file to S3
            s3.upload_file(tmp_path, bucket, s3_key, ExtraArgs=extra_args)
            # Generate a pre-signed URL
            presigned_url = s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": s3_key},
                ExpiresIn=self.presigned_expiration
            )
            return presigned_url
        except Exception as e:
            # You might integrate a logger here. For now we simply print an error.
            print(f"Failed to upload {local_path} to S3: {e}")
            return None


class GoogleDriveResource(ConfigurableResource):
    """A Dagster resource to handle Google Drive upload and download operations."""
    
    # Configurable parameter for the service account file path
    service_account_file_path: str
    folder: str

    def _build_service(self, scopes: typing.List[str]):
        """Creates a Drive API client with the given scopes."""
        credentials = service_account.Credentials.from_service_account_file(
            self.service_account_file_path,
            scopes=scopes
        )
        return build("drive", "v3", credentials=credentials)

    def get_file(self, file_id: str) -> io.BytesIO:
        """Download a single file from Google Drive as a BytesIO stream.
        
        Args:
            file_id (str): The Google Drive file ID.
            
        Returns:
            BytesIO: A file-like binary stream containing the file’s content.
        """
        service = self._build_service(["https://www.googleapis.com/auth/drive.readonly"])
        request = service.files().get_media(fileId=file_id)
        file_buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(file_buffer, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        file_buffer.seek(0)
        return file_buffer

    def upload_file(self, file_path: Path, mime_type: typing.Optional[str] = None) -> str:
        """
        Uploads a file to Google Drive and converts HTML or CSV files
        to Google Docs or Google Sheets formats respectively.

        Args:
            file_path (Path): The path to the local file.
            mime_type (Optional[str]): The MIME type to use for the file. If None,
                                    it will be auto-detected.

        Returns:
            str: The uploaded file's Google Drive ID.
        """
        # Build the Drive service with the required scope.
        service = self._build_service(["https://www.googleapis.com/auth/drive"])

        # Attempt to determine the MIME type of the file if not provided.
        if mime_type is None:
            mime_type, _ = mimetypes.guess_type(str(file_path))
            if mime_type is None:
                mime_type = "application/octet-stream"

        # Set up initial file metadata with the file name and parent folder.
        file_metadata = {"name": file_path.name, "parents": [self.folder]}
        
        # Check the file extension to decide if conversion is needed.
        if file_path.suffix.lower() == ".csv":
            # For CSV files, set the target MIME type to Google Sheets.
            file_metadata["mimeType"] = "application/vnd.google-apps.spreadsheet"
        elif file_path.suffix.lower() == ".html":
            # For HTML files, set the target MIME type to Google Docs.
            file_metadata["mimeType"] = "application/vnd.google-apps.document"

        # Create a MediaFileUpload object with the determined MIME type.
        media = MediaFileUpload(str(file_path), mimetype=mime_type)

        # Upload the file with the metadata; if a target Google Workspace MIME type was added,
        # Google Drive will perform the conversion as long as the file content is compatible.
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id"
        ).execute()

        return file.get("id")

    def list_files(self) -> typing.List[dict]:
        """Lists files (and folders) in a given Google Drive folder.
        
        Args:
            folder_id (str): The ID of the Google Drive folder.
            
        Returns:
            List[dict]: A list of file metadata dictionaries.
        """
        service = self._build_service(["https://www.googleapis.com/auth/drive.readonly"])
        query = f"'{self.folder}' in parents and trashed = false"
        results = service.files().list(q=query, fields="files(id, name, mimeType)").execute()
        files = results.get("files", [])
        return files

    def download_folder_recursive(self, output_path: str) -> None:
        """Recursively downloads all files and subfolders from the specified Google Drive folder.
        
        Args:
            folder_id (str): The ID of the Google Drive folder to download.
            output_path (str): The local directory path where files should be saved.
        """
        service = self._build_service(["https://www.googleapis.com/auth/drive.readonly"])
        os.makedirs(output_path, exist_ok=True)
        query = f"'{self.folder}' in parents and trashed = false"
        results = service.files().list(q=query, fields="files(id, name, mimeType)").execute()
        files = results.get("files", [])
        
        for file in files:
            file_name = file["name"]
            file_id = file["id"]
            file_path = os.path.join(output_path, file_name)
            
            # Log download information (alternatively use a logger if provided in context)
            print(f"Downloading file/folder {file_name} with ID {file_id}")
            
            if file["mimeType"] == "application/vnd.google-apps.folder":
                # For folders, recursively download contents
                self.download_folder_recursive(file_id, file_path)
            else:
                request = service.files().get_media(fileId=file_id)
                with open(file_path, "wb") as f:
                    downloader = MediaIoBaseDownload(f, request)
                    done = False
                    while not done:
                        status, done = downloader.next_chunk()

# An example resource for accessing local directories if needed
class LocalDirectoryResource(ConfigurableResource):
    """A Dagster resource to access files in a local directory."""
    path: str

    def get_path(self) -> Path:
        """Returns the directory path as a Path object."""
        return Path(self.path)
