# Data Platform End-to-End: NYC Taxi Medallion

แพลตฟอร์มข้อมูลแบบ local-first ที่ประมวลผล NYC TLC Yellow Taxi ตั้งแต่การดาวน์โหลดข้อมูล
จนถึง dashboard-ready marts โดยไม่ต้องใช้ paid cloud service

## Architecture

```text
NYC TLC HTTPS
    |
    v
Python ingestion -- checksum / manifest / schema validation
    |
    v
MinIO Bronze + local cache (immutable Parquet)
    |
    v
DuckDB + dbt
    |-- Silver: typed, deduplicated, quality status, quarantine
    `-- Gold: daily demand, zone performance, hourly pattern
    |
    +--> MinIO Silver/Gold Parquet
    `--> PostgreSQL --> Apache Superset

Apache Airflow controls retries, schedule, backfill, tests and publishing.
```

## Included components

- Apache Airflow with `LocalExecutor`
- MinIO S3-compatible data lake
- DuckDB and dbt Core transformations
- PostgreSQL Gold serving layer and pipeline audit log
- Apache Superset BI
- dbt and Great Expectations quality gates
- Prometheus/Grafana, Marquez/OpenLineage, Redpanda and Spark optional profiles
- pytest, Ruff, pre-commit and GitHub Actions CI
- NYC TLC Yellow Taxi 2024 (January starter plus sequential full-year backfill)

## Prerequisites

- Windows 11 with WSL2 and Docker Engine, Podman Desktop, or Rancher Desktop
- Docker Compose v2
- At least 16 GB RAM; 10 GB free disk for one month or 30 GB for the full-year/extensions
- Ports `15432`, `8080`, `8088`, `9000`, and `9001` available

Docker Desktop is free only under its license conditions. Docker Engine in WSL2, Podman Desktop,
or Rancher Desktop avoids requiring a Docker Desktop commercial subscription.

## Quick start

Generate a unique local `.env` before first startup. The file is ignored by Git:

```powershell
py -3.11 scripts\generate_env.py
docker compose build
docker compose up airflow-init
docker compose up -d
docker compose ps
```

Open:

- Airflow: <http://localhost:8080>
- MinIO Console: <http://localhost:9001>
- Superset: <http://localhost:8088>
- NYC Taxi dashboard: <http://localhost:8088/superset/dashboard/nyc-taxi-analytics/>

Usernames and generated passwords are in the local `.env`.

## Run January 2024 end-to-end

From the Airflow UI, enable `nyc_taxi_medallion`, select **Trigger DAG w/ config**, and use:

```json
{
  "year": 2024,
  "month": 1,
  "force_download": false
}
```

Or trigger it from PowerShell:

```powershell
docker compose exec airflow-scheduler airflow dags trigger nyc_taxi_medallion `
  --conf '{\"year\": 2024, \"month\": 1, \"force_download\": false}'
```

The first run downloads approximately three million rows. Subsequent runs reuse the local file and
skip MinIO uploads when the SHA-256 checksum has not changed.

## Pipeline tasks

1. `resolve_month` selects an explicitly requested month or two months behind the calendar,
   matching TLC's typical publication lag.
2. `source_is_available` skips cleanly when TLC has not published that partition.
3. `ingest` downloads the trip Parquet and taxi-zone CSV atomically.
4. Source schema, row count and SHA-256 are validated and written to a manifest.
5. Bronze files are uploaded to MinIO using deterministic object keys.
6. `dbt build` incrementally merges `fact_trips`, rebuilds marts and runs tests.
7. Great Expectations checks row counts, rejection rate and the 24-hour SLA.
8. Gold and Silver partitions are exported as compressed Parquet to MinIO.
9. Gold marts are transactionally replaced for the affected month in PostgreSQL.
10. Freshness, seven-run volume anomaly and quality results are recorded in audit details.

## Data-lake layout

```text
bronze/
  nyc_taxi/yellow/year=2024/month=01/yellow_tripdata_2024-01.parquet
  nyc_taxi/zones/taxi_zone_lookup.csv
  _manifests/year=2024/month=01/manifest.json
silver/
  fact_trips/year=2024/month=01/part-000.parquet
  silver_quarantine/year=2024/month=01/part-000.parquet
  dim_zone/year=2024/month=01/part-000.parquet
gold/
  mart_daily_demand/year=2024/month=01/part-000.parquet
  mart_zone_performance/year=2024/month=01/part-000.parquet
  mart_hourly_pattern/year=2024/month=01/part-000.parquet
  _manifests/year=2024/month=01/manifest.json
```

The same layout is cached under `lake/` on the host. This directory is ignored by Git.

## Data-quality policy

Silver assigns one `quality_status` per deduplicated trip:

- `accepted`
- `missing_timestamp`
- `invalid_duration`
- `duration_out_of_range`
- `distance_out_of_range`
- `amount_out_of_range`
- `missing_zone`

Invalid rows go to `silver_quarantine`; they are never silently deleted. `dbt build` fails if:

- IDs expected to be unique are duplicated
- required dimension keys are missing
- zone relationships are invalid
- the rejected proportion exceeds 5%

Great Expectations additionally verifies non-empty Bronze/Silver data, rejection rate and pipeline
freshness. A Bronze volume change above 30% from up to seven previous successful runs is preserved
as an audit warning.

## Superset dashboard

The one-shot `superset-bootstrap` service automatically and idempotently creates:

- PostgreSQL connection `NYC Taxi Analytics`
- datasets for the three Gold marts and pipeline audit table
- four KPI cards plus daily trend, top zones, weekday/hour heatmap and quality rate
- the published `NYC Taxi Analytics` dashboard

Run the bootstrap again after resetting Superset:

```powershell
docker compose run --rm superset-bootstrap
```

The script reuses resources by name and does not create duplicates. Ready-to-use SQL alternatives
are also available in `analytics/dashboard_queries.sql`.

## Run without Airflow

Start only the infrastructure:

```powershell
docker compose up -d postgres minio
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe scripts\run_local.py --year 2024 --month 1
```

The local runner reads `.env`, performs the same ingestion, executes `dbt build`, exports Parquet,
and publishes PostgreSQL marts.

## Tests and static checks

```powershell
.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\python.exe -m ruff check .
docker compose config --quiet
docker compose exec airflow-scheduler airflow dags list-import-errors
```

## Backfill another month

Backfill sequentially so only one process writes the local DuckDB file:

```powershell
.\scripts\task.ps1 backfill -Start 2024-01 -End 2024-12
```

Bronze objects are append-only, `fact_trips` uses a unique key, and publishing replaces only the
affected PostgreSQL month, so retries are idempotent.

## Optional profiles

These profiles are not required for the core pipeline and should be opened one at a time on a
16 GB machine.

```powershell
# Prometheus http://localhost:9090 and Grafana http://localhost:3001
.\scripts\task.ps1 observability

# Marquez API http://localhost:5000 and UI http://localhost:3002
.\scripts\task.ps1 lineage

# Replay and aggregate 10,000 trips through Redpanda
.\scripts\task.ps1 streaming

# Distributed Spark master/worker summary experiment
.\scripts\task.ps1 spark
```

Set `ALERT_WEBHOOK_URL` for pipeline failure notifications. The observability and lineage commands
enable Airflow StatsD/OpenLineage when recreating the scheduler.

## Automation and documentation

- `Makefile` provides equivalent commands for Unix/WSL.
- `scripts/task.ps1` is the Windows entry point.
- `scripts/acceptance.ps1` validates code, services and warehouse counts.
- `scripts/recovery_drill.ps1` simulates a MinIO outage and verifies idempotent recovery.
- `.github/workflows/ci.yml` runs tests, lint, dbt parse and Compose validation.
- `.pre-commit-config.yaml` enforces local checks.
- `docs/` contains architecture, ADR, data dictionary, runbook, extensions guide and demo script.

## Operations

View service state and logs:

```powershell
docker compose ps
docker compose logs --since=10m airflow-scheduler
docker compose logs --since=10m minio
```

Stop services without deleting data:

```powershell
docker compose down
```

Delete all local container data only when intentionally resetting the platform:

```powershell
docker compose down --volumes
Remove-Item -Recurse -Force lake, logs
```

The reset command is destructive. Bronze data can be downloaded again, but local audit history and
Superset configuration will be lost.

## Cost boundary

Every required component is self-hosted and has no required license or cloud usage fee. Hardware,
storage, internet and electricity remain real costs. Public GitHub Actions usage also depends on
repository visibility and current GitHub limits; local tests are the zero-cloud-cost default.
