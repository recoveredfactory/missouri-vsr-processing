from __future__ import annotations

import mimetypes
from pathlib import Path
from typing import Iterable, Optional


def _resolve_s3_target(context) -> tuple[str, str, object] | None:
    s3_res = getattr(context.resources, "s3", None)
    if s3_res is None:
        return None

    resolver_bucket = getattr(s3_res, "resolved_bucket", None)
    bucket = resolver_bucket() if callable(resolver_bucket) else getattr(s3_res, "bucket", None)
    if not bucket:
        return None

    resolver_prefix = getattr(s3_res, "resolved_prefix", None)
    prefix_clean = resolver_prefix() if callable(resolver_prefix) else (getattr(s3_res, "s3_prefix", "") or "")
    prefix_clean = prefix_clean.strip("/")
    dist_prefix = f"{prefix_clean}/dist" if prefix_clean else "dist"
    return bucket, dist_prefix, s3_res


def upload_file_with_presign(
    context,
    path: Path,
    key: str,
    *,
    content_type: str | None = None,
    expires_in: int | None = None,
) -> dict:
    """Upload a single file to S3 (prefix aware) and return metadata (s3_uri, presigned_url, errors)."""
    meta: dict = {}
    s3_res = getattr(context.resources, "s3", None)
    if s3_res is None:
        return meta

    resolver_bucket = getattr(s3_res, "resolved_bucket", None)
    bucket = resolver_bucket() if callable(resolver_bucket) else getattr(s3_res, "bucket", None)
    if not bucket:
        meta["s3_upload_error"] = "Missing bucket configuration."
        return meta

    resolver_prefix = getattr(s3_res, "resolved_prefix", None)
    prefix_clean = resolver_prefix() if callable(resolver_prefix) else (getattr(s3_res, "s3_prefix", "") or "")
    prefix_clean = prefix_clean.strip("/")
    key_prefix = f"{prefix_clean}/" if prefix_clean else ""
    key = f"{key_prefix}{key}"

    client = s3_res.client() if hasattr(s3_res, "client") else None
    if client is None:
        meta["s3_upload_error"] = "Missing S3 client."
        return meta

    extra_args = {}
    if content_type:
        extra_args["ContentType"] = content_type

    try:
        if extra_args:
            client.upload_file(str(path), bucket, key, ExtraArgs=extra_args)
        else:
            client.upload_file(str(path), bucket, key)
    except Exception as exc:
        meta["s3_upload_error"] = str(exc)
        return meta

    uri = f"s3://{bucket}/{key}"
    meta["s3_uri"] = uri

    try:
        expires = (
            int(expires_in)
            if expires_in is not None
            else int(getattr(s3_res, "presigned_expiration", 45 * 24 * 60 * 60))
        )
        presigned = client.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=expires,
        )
        meta["presigned_url"] = presigned
    except Exception as exc:
        meta["s3_presign_error"] = str(exc)

    return meta


def s3_uri_for_path(context, path: Path, base_dir: Path) -> Optional[str]:
    target = _resolve_s3_target(context)
    if not target:
        return None
    bucket, dist_prefix, _ = target
    base_dir = base_dir.resolve()
    try:
        rel = path.resolve().relative_to(base_dir)
    except ValueError:
        rel = Path(path.name)
    key = f"{dist_prefix}/{rel.as_posix()}"
    return f"s3://{bucket}/{key}"


def s3_uri_for_dir(context, path: Path, base_dir: Path) -> Optional[str]:
    uri = s3_uri_for_path(context, path, base_dir)
    if uri and not uri.endswith("/"):
        return f"{uri}/"
    return uri


def upload_paths(context, paths: Iterable[Path], base_dir: Path) -> list[str]:
    """Upload paths to S3 under <prefix>/dist/<relative_path>, if S3 is configured."""
    target = _resolve_s3_target(context)
    if not target:
        return []
    bucket, dist_prefix, s3_res = target
    client = s3_res.client() if hasattr(s3_res, "client") else None
    if client is None:
        return []

    uploaded_uris: list[str] = []
    base_dir = base_dir.resolve()
    for path in paths:
        path = Path(path)
        if not path.exists():
            continue
        try:
            rel = path.resolve().relative_to(base_dir)
        except ValueError:
            rel = path.name
        key = f"{dist_prefix}/{rel.as_posix()}"

        extra_args = {}
        content_type, _ = mimetypes.guess_type(str(path))
        if content_type:
            extra_args["ContentType"] = content_type

        if extra_args:
            client.upload_file(str(path), bucket, key, ExtraArgs=extra_args)
        else:
            client.upload_file(str(path), bucket, key)

        uri = f"s3://{bucket}/{key}"
        uploaded_uris.append(uri)
        context.log.debug("Uploaded %s → %s", path, uri)
    return uploaded_uris
