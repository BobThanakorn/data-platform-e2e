from __future__ import annotations

import argparse
import secrets
from pathlib import Path


def token(length: int = 32) -> str:
    return secrets.token_urlsafe(length)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate secure local service credentials.")
    parser.add_argument("--output", type=Path, default=Path(".env"))
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    if args.output.exists() and not args.force:
        raise FileExistsError(f"{args.output} already exists; pass --force to replace it")

    content = f"""POSTGRES_USER=platform
POSTGRES_PASSWORD={token()}
POSTGRES_HOST=localhost
POSTGRES_HOST_PORT=15432
POSTGRES_PORT=15432

MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD={token()}
MINIO_BUCKET_BRONZE=bronze
MINIO_BUCKET_SILVER=silver
MINIO_BUCKET_GOLD=gold

AIRFLOW_SECRET_KEY={token(48)}
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD={token()}

SUPERSET_SECRET_KEY={token(48)}
SUPERSET_ADMIN_USER=admin
SUPERSET_ADMIN_PASSWORD={token()}

GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD={token()}
STATSD_ON=false
OPENLINEAGE_DISABLED=true
ALERT_WEBHOOK_URL=

NYC_TAXI_YEAR=2024
NYC_TAXI_MONTH=01
"""
    args.output.write_text(content, encoding="utf-8")
    print(f"Wrote secure credentials to {args.output}")


if __name__ == "__main__":
    main()
