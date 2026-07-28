from __future__ import annotations

import logging
import os
import uuid
from datetime import UTC, datetime
from typing import Any

import requests

LOGGER = logging.getLogger(__name__)


def emit_openlineage_complete(metrics: dict[str, Any]) -> bool:
    """Emit dataset-level OpenLineage without coupling core Airflow dependencies."""
    if os.getenv("OPENLINEAGE_DISABLED", "true").lower() == "true":
        return False
    base_url = os.getenv("OPENLINEAGE_URL", "http://marquez-api:5000").rstrip("/")
    run_id = str(uuid.uuid5(uuid.NAMESPACE_URL, str(metrics["run_id"])))
    month = str(metrics["dataset_month"])
    event = {
        "eventType": "COMPLETE",
        "eventTime": datetime.now(UTC).isoformat(),
        "run": {"runId": run_id},
        "job": {
            "namespace": "data-platform-e2e",
            "name": "nyc_taxi_medallion",
        },
        "inputs": [
            {
                "namespace": "nyc-tlc",
                "name": f"yellow_tripdata_{month}.parquet",
            }
        ],
        "outputs": [
            {"namespace": "minio", "name": f"silver.fact_trips.{month}"},
            {"namespace": "postgresql", "name": "gold.mart_daily_demand"},
            {"namespace": "postgresql", "name": "gold.mart_zone_performance"},
            {"namespace": "postgresql", "name": "gold.mart_hourly_pattern"},
        ],
        "producer": "https://github.com/OpenLineage/OpenLineage",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
    }
    try:
        response = requests.post(f"{base_url}/api/v1/lineage", json=event, timeout=10)
        response.raise_for_status()
        return True
    except requests.RequestException:
        LOGGER.exception("Unable to emit OpenLineage event")
        return False
