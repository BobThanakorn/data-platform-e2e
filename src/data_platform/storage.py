from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError

from data_platform.config import Settings


def s3_client(settings: Settings):
    scheme = "https" if settings.minio_secure else "http"
    return boto3.client(
        "s3",
        endpoint_url=f"{scheme}://{settings.minio_endpoint}",
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        region_name="us-east-1",
    )


def ensure_buckets(settings: Settings) -> None:
    client = s3_client(settings)
    existing = {item["Name"] for item in client.list_buckets().get("Buckets", [])}
    for bucket in (settings.bronze_bucket, settings.silver_bucket, settings.gold_bucket):
        if bucket not in existing:
            client.create_bucket(Bucket=bucket)


def object_has_checksum(settings: Settings, bucket: str, key: str, sha256: str) -> bool:
    client = s3_client(settings)
    try:
        response = client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise
    return response.get("Metadata", {}).get("sha256") == sha256


def upload_file(
    settings: Settings,
    local_path: Path,
    bucket: str,
    key: str,
    sha256: str,
) -> bool:
    if object_has_checksum(settings, bucket, key, sha256):
        return False
    s3_client(settings).upload_file(
        str(local_path),
        bucket,
        key,
        ExtraArgs={"Metadata": {"sha256": sha256}},
    )
    return True


def put_json(settings: Settings, bucket: str, key: str, payload: dict[str, Any]) -> None:
    s3_client(settings).put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, indent=2, sort_keys=True).encode(),
        ContentType="application/json",
    )
