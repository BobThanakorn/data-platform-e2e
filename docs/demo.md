# Demo script

1. Show `docker compose ps` and explain that the core runs locally with no required cloud account.
2. Open Airflow and trigger January 2024.
3. Open the Bronze manifest to demonstrate source URL, checksum, row count and lineage metadata.
4. Show `silver.fact_trips` and `silver.silver_quarantine`.
5. Open the Superset dashboard and filter daily/zone/hour metrics.
6. Show `audit.pipeline_runs` with freshness, rejection and volume checks.
7. Start Grafana and Marquez profiles to demonstrate operations and lineage.
8. Replay 10,000 trips through Redpanda and query `streaming.trip_events_minute`.
9. Run the Spark profile and show its distributed daily summary output.
10. Finish with CI, pre-commit and the recovery procedure in the runbook.
