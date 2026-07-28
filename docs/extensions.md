# Optional Compose Profiles

Core pipeline services start with `docker compose up -d` only. Extension stacks are isolated behind
Compose profiles and do not block core health checks.

| Profile | Services | Host URLs | Task helper |
|---------|----------|-----------|-------------|
| `observability` | statsd-exporter, postgres-exporter, Prometheus, Grafana | :9090, :3001 | `.\scripts\task.ps1 observability` |
| `lineage` | marquez-db, marquez-api, marquez-web | :5000, :3002 | `.\scripts\task.ps1 lineage` |
| `streaming` | redpanda, stream-replay, stream-consumer | :19092 | `.\scripts\task.ps1 streaming` |
| `spark` | spark-master, spark-worker, spark-job | :7077, :8081 | `.\scripts\task.ps1 spark` |

Verification scripts:

- Lineage: `python scripts/verify_lineage.py`
- Superset BI: `python scripts/verify_superset.py`

Run one profile at a time on 16 GB machines. See `docs/runbook.md` for recovery and reset steps.
