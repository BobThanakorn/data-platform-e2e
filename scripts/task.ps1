param(
    [Parameter(Position = 0)]
    [ValidateSet("bootstrap", "up", "down", "test", "validate", "bi", "observability", "lineage", "streaming", "spark", "backfill", "recovery")]
    [string]$Task = "validate",
    [string]$Start = "2024-01",
    [string]$End = "2024-12"
)

$ErrorActionPreference = "Stop"

switch ($Task) {
    "bootstrap" {
        docker compose build
        docker compose up airflow-init
        docker compose up -d
        docker compose run --rm superset-bootstrap
    }
    "up" { docker compose up -d }
    "down" { docker compose down }
    "test" {
        .\.venv\Scripts\python.exe -m pytest
        .\.venv\Scripts\python.exe -m ruff check .
    }
    "validate" {
        .\.venv\Scripts\python.exe -m pytest
        .\.venv\Scripts\python.exe -m ruff check .
        docker compose config --quiet
        docker compose exec airflow-scheduler airflow dags list-import-errors
    }
    "bi" { docker compose run --rm superset-bootstrap }
    "observability" {
        $env:STATSD_ON = "true"
        docker compose --profile observability up -d
    }
    "lineage" {
        $env:OPENLINEAGE_DISABLED = "false"
        docker compose --profile lineage up -d --force-recreate airflow-scheduler
    }
    "streaming" {
        docker compose --profile streaming up -d redpanda stream-consumer
        docker compose --profile streaming run --rm stream-replay
    }
    "spark" {
        docker compose --profile spark up -d spark-master spark-worker
        docker compose --profile spark run --rm spark-job
    }
    "backfill" {
        .\.venv\Scripts\python.exe scripts\backfill.py --start $Start --end $End
    }
    "recovery" { .\scripts\recovery_drill.ps1 -Month $Start }
}
