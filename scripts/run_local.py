from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path

from data_platform.bootstrap import main as bootstrap
from data_platform.ingest import ingest_month
from data_platform.publish import publish_month


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the NYC Taxi pipeline without Airflow.")
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--month", type=int, default=1)
    parser.add_argument("--force-download", action="store_true")
    args = parser.parse_args()

    bootstrap()
    manifest = ingest_month(args.year, args.month, force=args.force_download)
    project_dir = Path(__file__).resolve().parents[1] / "dbt"
    variables = {
        "bronze_trip_path": manifest["trip_file"]["local_path"],
        "bronze_zone_path": manifest["zone_file"]["local_path"],
        "dataset_month": f"{args.year}-{args.month:02d}",
    }
    environment = os.environ.copy()
    environment.setdefault("DBT_PROFILES_DIR", str(project_dir))
    subprocess.run(
        [
            "dbt",
            "build",
            "--project-dir",
            str(project_dir),
            "--profiles-dir",
            str(project_dir),
            "--vars",
            json.dumps(variables),
        ],
        check=True,
        env=environment,
    )
    metrics = publish_month(
        args.year,
        args.month,
        pipeline_started_at=str(manifest["started_at"]),
    )
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
