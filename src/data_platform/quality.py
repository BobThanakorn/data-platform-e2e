from __future__ import annotations

import json
from pathlib import Path
from statistics import mean
from typing import Any

import great_expectations as gx
import pandas as pd
import psycopg2

from data_platform.config import Settings


def add_history_checks(settings: Settings, metrics: dict[str, Any]) -> dict[str, Any]:
    """Add freshness and seven-run volume anomaly results to pipeline metrics."""
    enriched = dict(metrics)
    started = pd.Timestamp(metrics["started_at"])
    completed = pd.Timestamp(metrics["completed_at"])
    duration_seconds = float((completed - started).total_seconds())
    enriched["duration_seconds"] = round(duration_seconds, 3)
    enriched["freshness_sla_hours"] = 24
    enriched["freshness_sla_met"] = duration_seconds <= 24 * 60 * 60

    with psycopg2.connect(settings.postgres_dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE SCHEMA IF NOT EXISTS audit;
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
                )
                """
            )
            cursor.execute(
                """
                SELECT bronze_rows
                FROM audit.pipeline_runs
                WHERE status = 'success' AND bronze_rows IS NOT NULL
                ORDER BY completed_at DESC
                LIMIT 7
                """
            )
            history = [int(row[0]) for row in cursor.fetchall()]

    if history:
        baseline = mean(history)
        deviation_pct = 100.0 * (int(metrics["bronze_rows"]) - baseline) / baseline
        enriched["volume_baseline_rows"] = round(baseline, 2)
        enriched["volume_deviation_pct"] = round(deviation_pct, 4)
        enriched["volume_anomaly_warning"] = abs(deviation_pct) > 30
    else:
        enriched["volume_baseline_rows"] = None
        enriched["volume_deviation_pct"] = None
        enriched["volume_anomaly_warning"] = False
    return enriched


def validate_with_great_expectations(
    metrics: dict[str, Any], output_path: Path
) -> dict[str, Any]:
    """Validate aggregate pipeline quality gates and persist a GX result."""
    frame = gx.from_pandas(pd.DataFrame([metrics]))
    results = [
        frame.expect_column_values_to_be_between("bronze_rows", min_value=1),
        frame.expect_column_values_to_be_between("silver_rows", min_value=1),
        frame.expect_column_values_to_be_between(
            "rejected_rate_pct", min_value=0, max_value=5
        ),
        frame.expect_column_values_to_be_in_set("freshness_sla_met", [True]),
    ]
    payload = {
        "success": all(bool(result["success"]) for result in results),
        "expectation_results": [result.to_json_dict() for result in results],
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    if not payload["success"]:
        raise ValueError(f"Great Expectations quality gate failed: {output_path}")
    return payload
