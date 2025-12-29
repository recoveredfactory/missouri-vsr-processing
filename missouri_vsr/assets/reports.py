from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from dagster import AssetIn, AssetKey, In, Out, graph_asset, op

from missouri_vsr.assets.extract import YEAR_URLS

# ------------------------------------------------------------------------------
# Combine all extracted DataFrame assets into one JSON and DataFrame
# ------------------------------------------------------------------------------
@op(
    ins={f"extract_pdf_data_{year}": In(pd.DataFrame) for year in YEAR_URLS},
    out=Out(pd.DataFrame),
    required_resource_keys={"data_dir_processed", "s3"},
)
def combine_reports(context, **extracted_reports: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Concatenate all extract_pdf_data_* assets into a single DataFrame and write combined Parquet."""
    # Merge all DataFrames
    dfs = [df for df in extracted_reports.values() if not df.empty]
    if not dfs:
        raise ValueError("No extracted tables found to combine.")
    combined = pd.concat(dfs, ignore_index=True)

    # Write combined Parquet
    processed_dir = Path(context.resources.data_dir_processed.get_path())
    out_file = processed_dir / "all_combined_output.parquet"
    combined.to_parquet(out_file, index=False, engine="pyarrow")
    context.log.info("Wrote combined Parquet: %d rows → %s", len(combined), out_file)

    # Attach local path and optional S3 metadata
    meta = {"local_path": str(out_file)}
    try:
        s3_res = getattr(context.resources, "s3", None)
        if s3_res is not None:
            # Resolve bucket/prefix from resource config or env.
            resolver_bucket = getattr(s3_res, "resolved_bucket", None)
            bucket = resolver_bucket() if callable(resolver_bucket) else getattr(s3_res, "bucket", None)
            if not bucket:
                bucket = os.getenv("MISSOURI_BUCKET_NAME") or os.getenv("AWS_S3_BUCKET")
            s3_prefix = getattr(s3_res, "resolved_prefix", None)
            prefix_clean = s3_prefix() if callable(s3_prefix) else (getattr(s3_res, "s3_prefix", "") or "").strip("/")

            if not bucket:
                context.log.warning("S3 resource present but no bucket configured (env MISSOURI_BUCKET_NAME/AWS_S3_BUCKET). Skipping upload.")
                meta["s3_upload_error"] = "Missing bucket configuration."
                context.add_output_metadata(meta)
                return combined

            context.log.info("S3 configured; uploading combined Parquet… [bucket=%s, prefix=%s]", bucket, prefix_clean)
            key_prefix = f"{prefix_clean}/" if prefix_clean else ""
            key = f"{key_prefix}combined/all_combined_output.parquet"
            import boto3
            from botocore.config import Config
            cfg = Config(signature_version="s3v4")
            region = getattr(s3_res, "resolved_region", lambda: None)()
            if region:
                client = boto3.client("s3", region_name=region, config=cfg)
            else:
                client = boto3.client("s3", config=cfg)
            try:
                client.upload_file(str(out_file), bucket, key, ExtraArgs={"ContentType": "application/vnd.apache.parquet"})
            except Exception as e:
                context.log.exception("S3 upload failed for combined Parquet: %s", e)
                meta["s3_upload_error"] = str(e)
            uri = f"s3://{bucket}/{key}"
            meta["s3_uri"] = uri
            try:
                # Default to 45 days if not configured.
                expires = int(getattr(s3_res, "presigned_expiration", 45 * 24 * 60 * 60))
                presigned = client.generate_presigned_url(
                    ClientMethod="get_object",
                    Params={"Bucket": bucket, "Key": key},
                    ExpiresIn=expires,
                )
                meta["presigned_url"] = presigned
                context.log.info("Generated presigned URL for combined Parquet")
            except Exception as e:
                context.log.exception("Presign failed for combined Parquet: %s", e)
                meta["s3_presign_error"] = str(e)
            context.log.info("Combined Parquet available at %s", uri)
        else:
            context.log.debug("No S3 resource configured; skipping S3 upload for combined Parquet")
    except Exception as e:
        context.log.exception("S3 handling encountered an exception for combined Parquet: %s", e)

    # Add the same row-count/uniques metadata as per-year extracts.
    try:
        meta["row_count"] = int(len(combined))
        if "Department" in combined.columns:
            meta["unique_agencies"] = int(combined["Department"].nunique(dropna=True))
        if "slug" in combined.columns:
            meta["unique_slugs"] = int(combined["slug"].nunique(dropna=True))
        context.add_output_metadata(meta)
        context.log.info("combine_all_reports metadata: %s", meta)
    except Exception:
        pass

    return combined


@graph_asset(
    name="combine_all_reports",
    group_name="processed",
    ins={
        f"extract_pdf_data_{year}": AssetIn(key=AssetKey(f"extract_pdf_data_{year}"))
        for year in YEAR_URLS
    },
    description="Combine all per-year extract_pdf_data_* assets into a single JSON and DataFrame."
)
def combine_all_reports(**extracted_reports: pd.DataFrame) -> pd.DataFrame:
    return combine_reports(**extracted_reports)
