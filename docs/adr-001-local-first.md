# ADR-001: Local-first analytical platform

Status: Accepted

## Context

The project must demonstrate an end-to-end data platform without mandatory license or cloud fees.
The starter dataset has millions to tens of millions of rows and must run on one developer machine.

## Decision

- Use MinIO and partitioned Parquet for the lake.
- Use DuckDB and dbt Core for single-node transformations.
- Use PostgreSQL as the BI serving database.
- Use Airflow LocalExecutor for orchestration.
- Keep Spark, Redpanda, Prometheus/Grafana and Marquez behind optional Compose profiles.

## Consequences

- The core is inexpensive and easy to reproduce.
- DuckDB avoids operating a compute cluster for the normal workload.
- PostgreSQL gives Superset stable concurrent SQL access.
- Optional profiles prove distributed, streaming, lineage and observability concepts but require
  additional memory.
- This design is educational and portfolio-ready; production deployment would add managed secrets,
  TLS, remote backups, high availability and organizational access controls.
