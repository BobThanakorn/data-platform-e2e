from __future__ import annotations

import argparse
import os
import time
from datetime import date

import requests
from dotenv import load_dotenv

load_dotenv()


def months_between(start: str, end: str) -> list[tuple[int, int]]:
    start_date = date.fromisoformat(f"{start}-01")
    end_date = date.fromisoformat(f"{end}-01")
    if start_date > end_date:
        raise ValueError("start month must not be after end month")
    months: list[tuple[int, int]] = []
    year, month = start_date.year, start_date.month
    while (year, month) <= (end_date.year, end_date.month):
        months.append((year, month))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return months


def wait_for_run(
    session: requests.Session,
    base_url: str,
    dag_id: str,
    run_id: str,
    poll_seconds: int,
) -> None:
    endpoint = f"{base_url}/api/v1/dags/{dag_id}/dagRuns/{run_id}"
    while True:
        try:
            response = session.get(endpoint, timeout=30)
            response.raise_for_status()
        except requests.RequestException as error:
            print(
                f"{run_id}: Airflow API temporarily unavailable ({error}); retrying",
                flush=True,
            )
            time.sleep(poll_seconds)
            continue
        state = response.json()["state"]
        print(f"{run_id}: {state}", flush=True)
        if state == "success":
            return
        if state in {"failed", "upstream_failed"}:
            raise RuntimeError(f"Airflow run failed: {run_id}")
        time.sleep(poll_seconds)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill sequential NYC Taxi monthly DAG runs.")
    parser.add_argument("--start", required=True, help="First month in YYYY-MM format")
    parser.add_argument("--end", required=True, help="Last month in YYYY-MM format")
    parser.add_argument("--airflow-url", default="http://localhost:8080")
    parser.add_argument("--username", default=os.getenv("AIRFLOW_ADMIN_USER", "admin"))
    parser.add_argument("--password", default=os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin"))
    parser.add_argument("--poll-seconds", type=int, default=10)
    parser.add_argument("--force-download", action="store_true")
    args = parser.parse_args()

    session = requests.Session()
    session.auth = (args.username, args.password)
    base_url = args.airflow_url.rstrip("/")
    dag_id = "nyc_taxi_medallion"

    for year, month in months_between(args.start, args.end):
        run_id = f"manual__backfill_{year}_{month:02d}__{int(time.time())}"
        response = session.post(
            f"{base_url}/api/v1/dags/{dag_id}/dagRuns",
            json={
                "dag_run_id": run_id,
                "conf": {
                    "year": year,
                    "month": month,
                    "force_download": args.force_download,
                },
            },
            timeout=30,
        )
        if not response.ok:
            raise RuntimeError(
                f"Unable to trigger {run_id} ({response.status_code}): {response.text}"
            )
        wait_for_run(session, base_url, dag_id, run_id, args.poll_seconds)


if __name__ == "__main__":
    main()
