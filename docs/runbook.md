# Operations runbook

## Daily checks

1. Run `docker compose ps`; all core services must be healthy.
2. Review the latest Airflow run and task logs.
3. Query `audit.pipeline_runs` for rejection rate, freshness and volume warnings.
4. Check the Superset data-quality charts.

## Source unavailable

The availability task intentionally skips downstream tasks when TLC has not published the expected
month. This is not data corruption. Retry after publication or trigger an explicit older month.

## Pipeline failure

1. Identify the first failed Airflow task.
2. Correct transient network/service problems and clear only the failed task.
3. Re-run safely: Bronze uploads use checksums, dbt facts use a unique key, and PostgreSQL replaces
   only the affected month.
4. If `dbt build` fails, inspect quarantine and the compiled SQL under `dbt/target`.

Set `ALERT_WEBHOOK_URL` to a Slack-compatible or generic webhook to receive failure callbacks.

## Data-quality failure

- Rejection rate above 5% fails Great Expectations and dbt quality gates.
- Volume deviation above 30% compared with up to seven previous successful runs is recorded as a
  warning in audit details.
- Keep rejected records; do not delete quarantine evidence.

## Backfill

Run sequentially to avoid concurrent writes to the single DuckDB file:

```powershell
.\scripts\task.ps1 backfill -Start 2024-01 -End 2024-12
```

## Recovery test

1. Stop MinIO.
2. Trigger a known month and confirm retries/failure alert.
3. Start MinIO and clear the failed task.
4. Confirm row counts match the previous manifest and no duplicate facts appear.

## Backup

Back up the named PostgreSQL, MinIO and Superset volumes plus `lake/manifests`. Bronze can be
downloaded again, but Superset configuration and audit history cannot be reconstructed exactly.

## Security

The checked-in `.env` is development-only and ignored by Git. Rotate all passwords, use random
secret keys, bind ports to localhost or a trusted network, and add TLS before remote exposure.
