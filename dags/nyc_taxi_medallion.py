from __future__ import annotations

import json
import logging
import os
import subprocess
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context

LOGGER = logging.getLogger(__name__)


def log_failure(context) -> None:
    task_instance = context["task_instance"]
    LOGGER.error(
        "Pipeline task failed dag=%s task=%s run=%s error=%s log=%s",
        task_instance.dag_id,
        task_instance.task_id,
        context.get("run_id"),
        context.get("exception"),
        task_instance.log_url,
    )
    webhook_url = os.getenv("ALERT_WEBHOOK_URL")
    if webhook_url:
        import requests

        try:
            requests.post(
                webhook_url,
                json={
                    "text": (
                        f"Airflow failure: {task_instance.dag_id}.{task_instance.task_id} "
                        f"run={context.get('run_id')} error={context.get('exception')}"
                    ),
                    "dag_id": task_instance.dag_id,
                    "task_id": task_instance.task_id,
                    "run_id": context.get("run_id"),
                    "log_url": task_instance.log_url,
                },
                timeout=10,
            ).raise_for_status()
        except requests.RequestException:
            LOGGER.exception("Unable to deliver failure webhook")


@dag(
    dag_id="nyc_taxi_medallion",
    description="Ingest and publish one NYC TLC Yellow Taxi month through Bronze/Silver/Gold.",
    schedule="0 6 5 * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    params={
        "year": Param(None, type=["null", "integer"], minimum=2009, maximum=2100),
        "month": Param(None, type=["null", "integer"], minimum=1, maximum=12),
        "force_download": Param(False, type="boolean"),
    },
    default_args={
        "owner": "data-platform",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=15),
        "on_failure_callback": log_failure,
    },
    tags=["nyc-taxi", "medallion", "batch"],
)
def nyc_taxi_medallion():
    @task(execution_timeout=timedelta(minutes=2))
    def resolve_month() -> dict[str, int | bool]:
        context = get_current_context()
        params = context["params"]
        if params.get("year") is not None and params.get("month") is not None:
            year = int(params["year"])
            month = int(params["month"])
        else:
            # TLC publication commonly trails the calendar by more than a month.
            target = context["data_interval_end"].subtract(months=2)
            year, month = target.year, target.month
        return {
            "year": year,
            "month": month,
            "force_download": bool(params.get("force_download", False)),
        }

    @task.short_circuit(execution_timeout=timedelta(minutes=2))
    def source_is_available(config: dict[str, int | bool]) -> bool:
        from data_platform.ingest import source_metadata

        metadata = source_metadata(int(config["year"]), int(config["month"]))
        if not metadata["available"]:
            LOGGER.warning(
                "TLC source is not published; downstream tasks will be skipped: %s",
                metadata,
            )
            return False
        LOGGER.info("TLC source is available: %s", metadata)
        return True

    @task(execution_timeout=timedelta(minutes=30))
    def ingest(config: dict[str, int | bool]) -> dict[str, object]:
        from data_platform.ingest import ingest_month

        manifest = ingest_month(
            int(config["year"]),
            int(config["month"]),
            force=bool(config["force_download"]),
        )
        return {
            "year": config["year"],
            "month": config["month"],
            "trip_path": manifest["trip_file"]["local_path"],
            "zone_path": manifest["zone_file"]["local_path"],
            "bronze_rows": manifest["trip_file"]["rows"],
            "pipeline_started_at": manifest["started_at"],
        }

    @task(execution_timeout=timedelta(minutes=45))
    def dbt_build(ingested: dict[str, object]) -> dict[str, object]:
        variables = {
            "bronze_trip_path": ingested["trip_path"],
            "bronze_zone_path": ingested["zone_path"],
            "dataset_month": f"{int(ingested['year'])}-{int(ingested['month']):02d}",
        }
        command = [
            "dbt",
            "build",
            "--project-dir",
            "/opt/airflow/dbt",
            "--profiles-dir",
            "/opt/airflow/dbt",
            "--vars",
            json.dumps(variables),
        ]
        subprocess.run(command, check=True)
        return ingested

    @task(execution_timeout=timedelta(minutes=20))
    def publish(built: dict[str, object]) -> dict[str, object]:
        from data_platform.publish import publish_month

        context = get_current_context()
        return publish_month(
            int(built["year"]),
            int(built["month"]),
            run_id=context["run_id"],
            pipeline_started_at=str(built["pipeline_started_at"]),
        )

    @task(execution_timeout=timedelta(minutes=2))
    def summarize(metrics: dict[str, object]) -> None:
        LOGGER.info(
            "Pipeline complete month=%s bronze=%s silver=%s rejected=%s rejected_rate=%s%%",
            metrics["dataset_month"],
            metrics["bronze_rows"],
            metrics["silver_rows"],
            metrics["rejected_rows"],
            metrics["rejected_rate_pct"],
        )

    config = resolve_month()
    available = source_is_available(config)
    bronze = ingest(config)
    available >> bronze
    built = dbt_build(bronze)
    metrics = publish(built)
    summarize(metrics)


nyc_taxi_medallion()
