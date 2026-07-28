$ErrorActionPreference = "Stop"

.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\python.exe -m ruff check .
docker compose config --quiet
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec postgres psql -U platform -d analytics -v ON_ERROR_STOP=1 -c @"
SELECT 'daily' AS model, count(*) FROM gold.mart_daily_demand
UNION ALL SELECT 'zone', count(*) FROM gold.mart_zone_performance
UNION ALL SELECT 'hourly', count(*) FROM gold.mart_hourly_pattern
UNION ALL SELECT 'audit', count(*) FROM audit.pipeline_runs;
"@

docker compose exec postgres psql -U platform -d analytics -v ON_ERROR_STOP=1 -c @"
SELECT min(trip_date), max(trip_date), count(*)
FROM gold.mart_daily_demand
WHERE trip_date >= DATE '2024-01-01' AND trip_date < DATE '2025-01-01';
"@

.\.venv\Scripts\python.exe scripts\validate_year.py
.\.venv\Scripts\python.exe scripts\verify_superset.py

$services = docker compose ps --format json | ConvertFrom-Json
$unhealthy = $services | Where-Object {
    $_.Service -in @("postgres", "minio", "airflow-webserver", "airflow-scheduler", "superset") -and
    $_.Health -ne "healthy"
}
if ($unhealthy) {
    throw "Core services are not healthy: $($unhealthy.Service -join ', ')"
}

$superset = Invoke-WebRequest -UseBasicParsing http://localhost:8088/health
if ($superset.StatusCode -ne 200) {
    throw "Superset health check failed"
}

Write-Host "Acceptance checks passed."
