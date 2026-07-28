from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import requests

from data_platform.config import ZONE_URL, Settings, trip_url, validate_month
from data_platform.storage import ensure_buckets, put_json, upload_file

REQUIRED_TRIP_COLUMNS = {
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "total_amount",
}


def source_metadata(year: int, month: int) -> dict[str, str | int | bool | None]:
    """Check whether TLC has published a monthly file without downloading it."""
    url = trip_url(year, month)
    response = requests.head(url, allow_redirects=True, timeout=(10, 30))
    return {
        "url": url,
        "available": response.status_code == 200,
        "status_code": response.status_code,
        "etag": response.headers.get("ETag"),
        "last_modified": response.headers.get("Last-Modified"),
        "content_length": response.headers.get("Content-Length"),
    }


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def download_atomic(
    url: str, destination: Path, force: bool = False
) -> dict[str, str | int | None]:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and not force:
        return {
            "url": url,
            "bytes": destination.stat().st_size,
            "etag": None,
            "last_modified": None,
            "downloaded": False,
        }

    temporary = destination.with_suffix(destination.suffix + ".part")
    try:
        with requests.get(url, stream=True, timeout=(10, 180)) as response:
            response.raise_for_status()
            with temporary.open("wb") as output:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        output.write(chunk)
            metadata: dict[str, str | int | None] = {
                "url": url,
                "bytes": temporary.stat().st_size,
                "etag": response.headers.get("ETag"),
                "last_modified": response.headers.get("Last-Modified"),
                "downloaded": True,
            }
        os.replace(temporary, destination)
        return metadata
    finally:
        temporary.unlink(missing_ok=True)


def validate_trip_file(path: Path) -> tuple[int, list[str]]:
    parquet = pq.ParquetFile(path)
    columns = parquet.schema_arrow.names
    missing = REQUIRED_TRIP_COLUMNS.difference(columns)
    if missing:
        raise ValueError(f"source schema is missing required columns: {sorted(missing)}")
    rows = parquet.metadata.num_rows
    if rows <= 0:
        raise ValueError("source parquet contains no rows")
    return rows, columns


def ingest_month(
    year: int,
    month: int,
    *,
    settings: Settings | None = None,
    force: bool = False,
) -> dict[str, Any]:
    validate_month(year, month)
    settings = settings or Settings()
    ensure_buckets(settings)

    month_id = f"{year}-{month:02d}"
    partition = Path(f"nyc_taxi/yellow/year={year}/month={month:02d}")
    trip_path = settings.lake_root / "bronze" / partition / f"yellow_tripdata_{month_id}.parquet"
    zone_path = settings.lake_root / "bronze" / "nyc_taxi/zones/taxi_zone_lookup.csv"

    started_at = datetime.now(UTC)
    trip_download = download_atomic(trip_url(year, month), trip_path, force)
    zone_download = download_atomic(ZONE_URL, zone_path, force)
    row_count, columns = validate_trip_file(trip_path)
    trip_checksum = sha256_file(trip_path)
    zone_checksum = sha256_file(zone_path)

    trip_key = partition.as_posix() + f"/yellow_tripdata_{month_id}.parquet"
    zone_key = "nyc_taxi/zones/taxi_zone_lookup.csv"
    trip_uploaded = upload_file(
        settings, trip_path, settings.bronze_bucket, trip_key, trip_checksum
    )
    zone_uploaded = upload_file(
        settings, zone_path, settings.bronze_bucket, zone_key, zone_checksum
    )

    manifest: dict[str, Any] = {
        "dataset": "nyc_tlc_yellow_taxi",
        "dataset_month": month_id,
        "status": "ready",
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "trip_file": {
            **trip_download,
            "local_path": str(trip_path.resolve()),
            "object_uri": f"s3://{settings.bronze_bucket}/{trip_key}",
            "sha256": trip_checksum,
            "rows": row_count,
            "columns": columns,
            "uploaded": trip_uploaded,
        },
        "zone_file": {
            **zone_download,
            "local_path": str(zone_path.resolve()),
            "object_uri": f"s3://{settings.bronze_bucket}/{zone_key}",
            "sha256": zone_checksum,
            "uploaded": zone_uploaded,
        },
    }

    manifest_path = settings.lake_root / "manifests" / f"bronze_{month_id}.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    put_json(
        settings,
        settings.bronze_bucket,
        f"_manifests/year={year}/month={month:02d}/manifest.json",
        manifest,
    )
    return manifest


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Ingest one NYC TLC Yellow Taxi month.")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    print(json.dumps(ingest_month(args.year, args.month, force=args.force), indent=2))
