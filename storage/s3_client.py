"""
AWS S3 client for data lake operations.
"""

import logging
from pathlib import Path
from typing import List, Optional

import boto3
from botocore.exceptions import ClientError

from config.settings import Settings

logger = logging.getLogger("ais_pipeline.s3_client")


class S3Client:
    """Wrapper around boto3 S3 client for pipeline operations."""

    def __init__(self, bucket: str = None, region: str = None):
        self.bucket = bucket or Settings.S3_BUCKET
        self.region = region or Settings.S3_REGION
        self.client = boto3.client("s3", region_name=self.region)

    def upload_file(self, local_path: Path, s3_key: str) -> bool:
        """Upload a single file to S3."""
        try:
            self.client.upload_file(str(local_path), self.bucket, s3_key)
            logger.info("Uploaded %s -> s3://%s/%s", local_path, self.bucket, s3_key)
            return True
        except ClientError as e:
            logger.error("Failed to upload %s: %s", local_path, e)
            return False

    def upload_directory(self, local_dir: Path, s3_prefix: str) -> int:
        """Upload all files in a directory to S3."""
        count = 0
        for file_path in local_dir.rglob("*"):
            if file_path.is_file():
                relative = file_path.relative_to(local_dir)
                s3_key = f"{s3_prefix}/{relative.as_posix()}"
                if self.upload_file(file_path, s3_key):
                    count += 1
        logger.info("Uploaded %d files to s3://%s/%s", count, self.bucket, s3_prefix)
        return count

    def list_keys(self, prefix: str) -> List[str]:
        """List object keys under a prefix."""
        keys = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys

    def download_file(self, s3_key: str, local_path: Path) -> bool:
        """Download a single file from S3."""
        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            self.client.download_file(self.bucket, s3_key, str(local_path))
            logger.info("Downloaded s3://%s/%s -> %s", self.bucket, s3_key, local_path)
            return True
        except ClientError as e:
            logger.error("Failed to download %s: %s", s3_key, e)
            return False

    def key_exists(self, s3_key: str) -> bool:
        """Check if an S3 key exists."""
        try:
            self.client.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except ClientError:
            return False
