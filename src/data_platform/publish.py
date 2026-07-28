from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import psycopg2
from psycopg2.extras import Json, execute_values

from data_platform.config import Settings, validate_month
from data_platform.ingest import sha256_file
from data_platform.lineage import emit_openlineage_complete
from data_platform.quality import add_history_checks, validate_with_great_expectations
from data_platform.storage import ensure_buckets, put_json, upload_file

MODEL_COLUMNS = {
    "mart_daily_demand": [
        "trip_date",
        "trip_count",
        "passenger_count",
        "distance_miles",
        "revenue_amount",
        "average_fare_amount",
        "average_duration_minutes",
        "tip_rate_pct",
    ],
    "mart_zone_performance": [
        "trip_date",
        "zone_id",
        "borough",
        "zone_name",
        "trip_count",
        "revenue_amount",
        "average_fare_amount",
        "average_duration_minutes",
        "average_speed_mph",
    ],
    "mart_hourly_pattern": [
        "dataset_month",
        "weekday_name",
        "weekday_number",
        "pickup_hour",
        "trip_count",
        "average_fare_amount",
        "average_duration_minutes",
        "tip_rate_pct",
    ],
}

GOLD_DDL = """
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS gold.mart_daily_demand (
    trip_date date PRIMARY KEY,
    trip_count bigint NOT NULL,
    passenger_count bigint,
    distance_miles numeric,
    revenue_amount numeric,
    average_fare_amount numeric,
    average_duration_minutes numeric,
    tip_rate_pct numeric
);

CREATE TABLE IF NOT EXISTS gold.mart_zone_performance (
    trip_date date NOT NULL,
    zone_id integer NOT NULL,
    borough text,
    zone_name text,
    trip_count bigint NOT NULL,
    revenue_amount numeric,
    average_fare_amount numeric,
    average_duration_minutes numeric,
    average_speed_mph numeric,
    PRIMARY KEY (trip_date, zone_id)
);

CREATE TABLE IF NOT EXISTS gold.mart_hourly_pattern (
    dataset_month text NOT NULL,
    weekday_name text NOT NULL,
    weekday_number integer NOT NULL,
    pickup_hour integer NOT NULL,
    trip_count bigint NOT NULL,
    average_fare_amount numeric,
    average_duration_minutes numeric,
    tip_rate_pct numeric,
    PRIMARY KEY (dataset_month, weekday_number, pickup_hour)
);

CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    run_id text PRIMARY KEY,
    dataset_month text NOT NULL,
    status text NOT NULL,
    bronze_rows bigint,
    silver_rows bigint,
    rejected_rows bigint,
    started_at timestamptz NOT NULL,
    completed_at timestamptz,
    details jsonb NOT NULL DEFAULT '{}'::jsonb
);
"""


def _copy_to_parquet(
    connection: duckdb.DuckDBPyConnection,
    model: str,
    destination: Path,
    where: str | None = None,
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(".parquet.part")
    temporary.unlink(missing_ok=True)
    escaped_path = str(temporary).replace("\\", "/").replace("'", "''")
    predicate = f" WHERE {where}" if where else ""
    connection.execute(
        f"COPY (SELECT * FROM {model}{predicate}) TO '{escaped_path}' "
        "(FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    temporary.replace(destination)


def _rows(
    connection: duckdb.DuckDBPyConnection, model: str, where: str | None = None
) -> list[tuple[Any, ...]]:
    predicate = f" WHERE {where}" if where else ""
    return connection.execute(f"SELECT * FROM {model}{predicate}").fetchall()


def _count(
    connection: duckdb.DuckDBPyConnection, model: str, where: str | None = None
) -> int:
    predicate = f" WHERE {where}" if where else ""
    return int(connection.execute(f"SELECT count(*) FROM {model}{predicate}").fetchone()[0])


def _publish_postgres(
    settings: Settings,
    year: int,
    month: int,
    run_id: str,
    rows_by_model: dict[str, list[tuple[Any, ...]]],
    metrics: dict[str, Any],
) -> None:
    month_id = f"{year}-{month:02d}"
    month_start = f"{month_id}-01"
    next_year, next_month = (year + 1, 1) if month == 12 else (year, month + 1)
    next_month_start = f"{next_year}-{next_month:02d}-01"

    with psycopg2.connect(settings.postgres_dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(GOLD_DDL)
            cursor.execute(
                "DELETE FROM gold.mart_daily_demand WHERE trip_date >= %s AND trip_date < %s",
                (month_start, next_month_start),
            )
            cursor.execute(
                "DELETE FROM gold.mart_zone_performance WHERE trip_date >= %s AND trip_date < %s",
                (month_start, next_month_start),
            )
            cursor.execute(
                "DELETE FROM gold.mart_hourly_pattern WHERE dataset_month = %s", (month_id,)
            )

            for model, rows in rows_by_model.items():
                if not rows:
                    continue
                columns = MODEL_COLUMNS[model]
                execute_values(
                    cursor,
                    f"INSERT INTO gold.{model} ({', '.join(columns)}) VALUES %s",
                    rows,
                    page_size=1000,
                )

            cursor.execute(
                """
                INSERT INTO audit.pipeline_runs (
                    run_id, dataset_month, status, bronze_rows, silver_rows,
                    rejected_rows, started_at, completed_at, details
                )
                VALUES (%s, %s, 'success', %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    bronze_rows = EXCLUDED.bronze_rows,
                    silver_rows = EXCLUDED.silver_rows,
                    rejected_rows = EXCLUDED.rejected_rows,
                    completed_at = EXCLUDED.completed_at,
                    details = EXCLUDED.details
                """,
                (
                    run_id,
                    month_id,
                    metrics["bronze_rows"],
                    metrics["silver_rows"],
                    metrics["rejected_rows"],
                    metrics["started_at"],
                    metrics["completed_at"],
                    Json(metrics),
                ),
            )


def publish_month(
    year: int,
    month: int,
    *,
    settings: Settings | None = None,
    run_id: str | None = None,
    pipeline_started_at: str | None = None,
) -> dict[str, Any]:
    validate_month(year, month)
    settings = settings or Settings()
    ensure_buckets(settings)
    run_id = run_id or str(uuid.uuid4())
    month_id = f"{year}-{month:02d}"
    next_year, next_month = (year + 1, 1) if month == 12 else (year, month + 1)
    next_month_id = f"{next_year}-{next_month:02d}"
    started_at = (
        datetime.fromisoformat(pipeline_started_at)
        if pipeline_started_at
        else datetime.now(UTC)
    )
    database_path = settings.lake_root / "platform.duckdb"
    if not database_path.exists():
        raise FileNotFoundError(f"DuckDB database does not exist: {database_path}")

    exported: list[dict[str, Any]] = []
    rows_by_model: dict[str, list[tuple[Any, ...]]] = {}
    with duckdb.connect(str(database_path), read_only=True) as connection:
        bronze_rows = connection.execute("SELECT count(*) FROM staging.stg_trips").fetchone()[0]
        silver_rows = connection.execute(
            "SELECT count(*) FROM silver.fact_trips WHERE dataset_month = ?", [month_id]
        ).fetchone()[0]
        rejected_rows = connection.execute(
            "SELECT count(*) FROM silver.silver_quarantine"
        ).fetchone()[0]

        exports = [
            ("silver.fact_trips", settings.silver_bucket, f"dataset_month = '{month_id}'"),
            ("silver.silver_quarantine", settings.silver_bucket, None),
            ("silver.dim_zone", settings.silver_bucket, None),
            (
                "gold.mart_daily_demand",
                settings.gold_bucket,
                f"trip_date >= DATE '{month_id}-01' AND trip_date < DATE '{next_month_id}-01'",
            ),
            (
                "gold.mart_zone_performance",
                settings.gold_bucket,
                f"trip_date >= DATE '{month_id}-01' AND trip_date < DATE '{next_month_id}-01'",
            ),
            ("gold.mart_hourly_pattern", settings.gold_bucket, f"dataset_month = '{month_id}'"),
        ]
        for qualified_model, bucket, where in exports:
            layer, model = qualified_model.split(".")
            destination = (
                settings.lake_root
                / layer
                / model
                / f"year={year}"
                / f"month={month:02d}"
                / "part-000.parquet"
            )
            _copy_to_parquet(connection, qualified_model, destination, where)
            checksum = sha256_file(destination)
            key = (
                f"{model}/year={year}/month={month:02d}/part-000.parquet"
            )
            uploaded = upload_file(settings, destination, bucket, key, checksum)
            exported.append(
                {
                    "model": qualified_model,
                    "rows": _count(connection, qualified_model, where),
                    "local_path": str(destination.resolve()),
                    "object_uri": f"s3://{bucket}/{key}",
                    "sha256": checksum,
                    "uploaded": uploaded,
                }
            )
            if layer == "gold":
                rows_by_model[model] = _rows(connection, qualified_model, where)

    completed_at = datetime.now(UTC)
    metrics: dict[str, Any] = {
        "run_id": run_id,
        "dataset_month": month_id,
        "status": "success",
        "bronze_rows": bronze_rows,
        "silver_rows": silver_rows,
        "rejected_rows": rejected_rows,
        "rejected_rate_pct": round(100 * rejected_rows / max(bronze_rows, 1), 4),
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "exports": exported,
    }
    metrics = add_history_checks(settings, metrics)
    quality_path = settings.lake_root / "quality" / f"gx_{month_id}.json"
    quality_result = validate_with_great_expectations(metrics, quality_path)
    metrics["great_expectations_success"] = quality_result["success"]
    metrics["great_expectations_result_path"] = str(quality_path.resolve())
    metrics["openlineage_emitted"] = emit_openlineage_complete(metrics)
    _publish_postgres(settings, year, month, run_id, rows_by_model, metrics)
    manifest_key = f"_manifests/year={year}/month={month:02d}/manifest.json"
    put_json(settings, settings.gold_bucket, manifest_key, metrics)
    manifest_path = settings.lake_root / "manifests" / f"publish_{month_id}.json"
    manifest_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    return metrics
