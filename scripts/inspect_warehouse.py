from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect incremental DuckDB facts.")
    parser.add_argument("--database", default="lake/platform.duckdb")
    parser.add_argument("--month", default="2024-01")
    args = parser.parse_args()

    with duckdb.connect(str(Path(args.database)), read_only=True) as connection:
        row = connection.execute(
            """
            SELECT
                count(*) AS rows,
                count(DISTINCT trip_id) AS unique_trips,
                typeof(min(pickup_at)) AS pickup_type
            FROM silver.fact_trips
            WHERE dataset_month = ?
            """,
            [args.month],
        ).fetchone()
    print({"rows": row[0], "unique_trips": row[1], "pickup_type": row[2]})


if __name__ == "__main__":
    main()
