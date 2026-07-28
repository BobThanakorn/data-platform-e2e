from __future__ import annotations

import json
import os
from pathlib import Path

import requests

from data_platform.lineage import emit_openlineage_complete


def main() -> None:
    manifests = sorted(
        Path("lake/manifests").glob("publish_*.json"),
        key=lambda path: path.stat().st_mtime,
    )
    if not manifests:
        raise FileNotFoundError("No publish manifest exists")
    metrics = json.loads(manifests[-1].read_text(encoding="utf-8"))
    os.environ["OPENLINEAGE_DISABLED"] = "false"
    os.environ["OPENLINEAGE_URL"] = "http://localhost:5000"
    if not emit_openlineage_complete(metrics):
        raise RuntimeError("OpenLineage event was not accepted")

    response = requests.get(
        "http://localhost:5000/api/v1/jobs",
        params={"namespace": "data-platform-e2e"},
        timeout=30,
    )
    response.raise_for_status()
    jobs = response.json()["jobs"]
    job = next(item for item in jobs if item["name"] == "nyc_taxi_medallion")
    print(
        json.dumps(
            {
                "job": job["name"],
                "namespace": job["namespace"],
                "latest_run": job.get("latestRun", {}).get("id"),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
