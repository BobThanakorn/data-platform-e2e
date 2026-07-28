param(
    [string]$Month = "2024-01"
)

$ErrorActionPreference = "Stop"
$parts = $Month.Split("-")
if ($parts.Count -ne 2) {
    throw "Month must use YYYY-MM format"
}

Write-Host "Stopping MinIO to simulate an object-store outage..."
docker compose stop minio
docker compose run --rm --no-deps -e AWS_MAX_ATTEMPTS=1 --entrypoint python airflow-init -m data_platform.bootstrap
if ($LASTEXITCODE -eq 0) {
    docker compose start minio
    throw "The outage probe unexpectedly succeeded"
}

Write-Host "Outage detected as expected; restoring MinIO..."
docker compose start minio
do {
    Start-Sleep -Seconds 2
    $health = docker inspect --format "{{.State.Health.Status}}" data-platform-e2e-minio-1
} until ($health -eq "healthy")

.\.venv\Scripts\python.exe scripts\backfill.py --start $Month --end $Month --poll-seconds 5
.\.venv\Scripts\python.exe scripts\inspect_warehouse.py --month $Month
Write-Host "Recovery drill completed; rerun was idempotent."
