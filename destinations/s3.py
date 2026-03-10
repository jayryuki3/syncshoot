"""S3-compatible cloud destination handler.

Supports AWS S3, Backblaze B2, MinIO, and other S3-compatible services.

Features:
- Multipart upload for large files with progress tracking
- Checksum verification on upload (Content-MD5)
- Configurable endpoint, bucket, prefix, storage class
- List, delete, and download operations
"""

from __future__ import annotations

import hashlib
import base64
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

from config import (
    DestinationType,
    S3_MULTIPART_THRESHOLD,
    S3_MULTIPART_CHUNKSIZE,
)


@dataclass
class S3Config:
    """Configuration for an S3-compatible destination."""
    bucket: str
    prefix: str = ""
    endpoint_url: Optional[str] = None      # None = AWS default
    region: str = "us-east-1"
    access_key: str = ""
    secret_key: str = ""
    storage_class: str = "STANDARD"
    multipart_threshold: int = S3_MULTIPART_THRESHOLD
    multipart_chunksize: int = S3_MULTIPART_CHUNKSIZE
    verify_upload: bool = True


# Progress: (bytes_uploaded, total_bytes)
S3ProgressCB = Callable[[int, int], None]


class S3Destination:
    """Handler for S3-compatible cloud destinations."""

    def __init__(self, config: S3Config):
        self.config = config
        self.dest_type = DestinationType.S3
        self._client = None

    @property
    def client(self):
        """Lazy-init boto3 client."""
        if self._client is None:
            import boto3
            kwargs = {
                "service_name": "s3",
                "region_name": self.config.region,
            }
            if self.config.endpoint_url:
                kwargs["endpoint_url"] = self.config.endpoint_url
            if self.config.access_key and self.config.secret_key:
                kwargs["aws_access_key_id"] = self.config.access_key
                kwargs["aws_secret_access_key"] = self.config.secret_key
            self._client = boto3.client(**kwargs)
        return self._client

    def _s3_key(self, rel_path: str) -> str:
        """Build the full S3 key from prefix + relative path."""
        if self.config.prefix:
            return f"{self.config.prefix.rstrip('/')}/{rel_path}"
        return rel_path

    # -- Upload ----------------------------------------------------------------
    def upload_file(
        self,
        local_path: Path,
        rel_path: str,
        progress_cb: Optional[S3ProgressCB] = None,
    ) -> dict:
        """Upload a file to S3 with optional multipart and verification.

        Returns dict with 'key', 'size', 'etag', 'md5' fields.
        """
        local_path = Path(local_path)
        file_size = local_path.stat().st_size
        key = self._s3_key(rel_path)

        extra_args = {"StorageClass": self.config.storage_class}

        # Compute MD5 for verification
        md5_digest = None
        if self.config.verify_upload:
            md5 = hashlib.md5()
            with open(local_path, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    md5.update(chunk)
            md5_digest = base64.b64encode(md5.digest()).decode()
            extra_args["ContentMD5"] = md5_digest

        # Progress callback wrapper
        uploaded = [0]

        def _progress(bytes_amount):
            uploaded[0] += bytes_amount
            if progress_cb:
                progress_cb(uploaded[0], file_size)

        callback = _progress if progress_cb else None

        if file_size > self.config.multipart_threshold:
            # Multipart upload
            from boto3.s3.transfer import TransferConfig
            transfer_config = TransferConfig(
                multipart_threshold=self.config.multipart_threshold,
                multipart_chunksize=self.config.multipart_chunksize,
            )
            self.client.upload_file(
                str(local_path), self.config.bucket, key,
                ExtraArgs=extra_args, Callback=callback,
                Config=transfer_config,
            )
        else:
            self.client.upload_file(
                str(local_path), self.config.bucket, key,
                ExtraArgs=extra_args, Callback=callback,
            )

        # Get ETag for confirmation
        head = self.client.head_object(Bucket=self.config.bucket, Key=key)

        return {
            "key": key,
            "size": file_size,
            "etag": head.get("ETag", "").strip('"'),
            "md5": md5_digest,
        }

    # -- Download --------------------------------------------------------------
    def download_file(
        self,
        rel_path: str,
        local_path: Path,
        progress_cb: Optional[S3ProgressCB] = None,
    ) -> Path:
        """Download a file from S3."""
        key = self._s3_key(rel_path)
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        head = self.client.head_object(Bucket=self.config.bucket, Key=key)
        total = head["ContentLength"]
        downloaded = [0]

        def _progress(bytes_amount):
            downloaded[0] += bytes_amount
            if progress_cb:
                progress_cb(downloaded[0], total)

        self.client.download_file(
            self.config.bucket, key, str(local_path),
            Callback=_progress if progress_cb else None,
        )
        return local_path

    # -- List ------------------------------------------------------------------
    def list_files(self, prefix: str = "") -> list[dict]:
        """List objects under prefix. Returns list of {key, size, last_modified}."""
        full_prefix = self._s3_key(prefix)
        results = []
        paginator = self.client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=self.config.bucket, Prefix=full_prefix):
            for obj in page.get("Contents", []):
                results.append({
                    "key": obj["Key"],
                    "size": obj["Size"],
                    "last_modified": obj["LastModified"].isoformat(),
                })
        return results

    # -- Delete ----------------------------------------------------------------
    def delete_file(self, rel_path: str) -> bool:
        """Delete a single object."""
        key = self._s3_key(rel_path)
        try:
            self.client.delete_object(Bucket=self.config.bucket, Key=key)
            return True
        except Exception:
            return False

    def delete_files(self, rel_paths: list[str]) -> int:
        """Batch delete. Returns count of deleted objects."""
        objects = [{"Key": self._s3_key(rp)} for rp in rel_paths]
        if not objects:
            return 0
        # S3 batch delete max 1000
        deleted = 0
        for i in range(0, len(objects), 1000):
            batch = objects[i:i + 1000]
            resp = self.client.delete_objects(
                Bucket=self.config.bucket,
                Delete={"Objects": batch, "Quiet": True},
            )
            deleted += len(batch) - len(resp.get("Errors", []))
        return deleted

    # -- Exists ----------------------------------------------------------------
    def file_exists(self, rel_path: str) -> bool:
        key = self._s3_key(rel_path)
        try:
            self.client.head_object(Bucket=self.config.bucket, Key=key)
            return True
        except self.client.exceptions.ClientError:
            return False
        except Exception:
            return False

    def file_size(self, rel_path: str) -> int:
        key = self._s3_key(rel_path)
        try:
            head = self.client.head_object(Bucket=self.config.bucket, Key=key)
            return head["ContentLength"]
        except Exception:
            return 0
