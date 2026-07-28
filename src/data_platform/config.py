from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    lake_root: Path = Path(os.getenv("LAKE_ROOT", "lake"))
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    minio_access_key: str = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
    minio_secure: bool = os.getenv("MINIO_SECURE", "false").lower() == "true"
    bronze_bucket: str = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
    silver_bucket: str = os.getenv("MINIO_BUCKET_SILVER", "silver")
    gold_bucket: str = os.getenv("MINIO_BUCKET_GOLD", "gold")
    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_db: str = os.getenv("POSTGRES_DB", "analytics")
    postgres_user: str = os.getenv("POSTGRES_USER", "platform")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "platform_dev_password")

    @property
    def postgres_dsn(self) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} dbname={self.postgres_db} "
            f"user={self.postgres_user} password={self.postgres_password}"
        )


def validate_month(year: int, month: int) -> tuple[int, int]:
    if year < 2009 or year > 2100:
        raise ValueError("year must be between 2009 and 2100")
    if month < 1 or month > 12:
        raise ValueError("month must be between 1 and 12")
    return year, month


def trip_url(year: int, month: int) -> str:
    validate_month(year, month)
    return (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        f"yellow_tripdata_{year}-{month:02d}.parquet"
    )


ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
