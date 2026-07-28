from __future__ import annotations

from pathlib import Path

import duckdb
import psycopg2

from data_platform.config import Settings


def main() -> None:
    database = Path("lake/platform.duckdb")
    with duckdb.connect(str(database)) as connection:
        months = [
            row[0]
            for row in connection.execute(
                "SELECT DISTINCT dataset_month FROM silver.fact_trips ORDER BY 1"
            ).fetchall()
        ]
        if not months:
            raise RuntimeError("No fact months found; refusing to delete Gold data")
        placeholders = ", ".join("?" for _ in months)
        deleted_duckdb = {}
        for model in ("mart_daily_demand", "mart_zone_performance"):
            before = connection.execute(f"SELECT count(*) FROM gold.{model}").fetchone()[0]
            connection.execute(
                f"DELETE FROM gold.{model} "
                f"WHERE strftime(trip_date, '%Y-%m') NOT IN ({placeholders})",
                months,
            )
            after = connection.execute(f"SELECT count(*) FROM gold.{model}").fetchone()[0]
            deleted_duckdb[model] = before - after

    settings = Settings()
    deleted_postgres = {}
    with psycopg2.connect(settings.postgres_dsn) as connection:
        with connection.cursor() as cursor:
            for model in ("mart_daily_demand", "mart_zone_performance"):
                cursor.execute(
                    f"DELETE FROM gold.{model} "
                    "WHERE NOT (to_char(trip_date, 'YYYY-MM') = ANY(%s))",
                    (months,),
                )
                deleted_postgres[model] = cursor.rowcount

    print(
        {
            "valid_months": months,
            "deleted_duckdb": deleted_duckdb,
            "deleted_postgres": deleted_postgres,
        }
    )


if __name__ == "__main__":
    main()
