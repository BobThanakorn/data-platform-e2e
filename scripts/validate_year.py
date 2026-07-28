from __future__ import annotations

import json
from pathlib import Path

import duckdb


def main() -> None:
    with duckdb.connect(str(Path("lake/platform.duckdb")), read_only=True) as connection:
        fact = connection.execute(
            """
            SELECT
                count(*) AS rows,
                count(DISTINCT trip_id) AS unique_trips,
                count(DISTINCT dataset_month) AS months,
                min(dataset_month) AS first_month,
                max(dataset_month) AS last_month,
                typeof(min(pickup_at)) AS pickup_type
            FROM silver.fact_trips
            WHERE dataset_month BETWEEN '2024-01' AND '2024-12'
            """
        ).fetchone()
        gold = connection.execute(
            """
            SELECT count(*), min(trip_date), max(trip_date)
            FROM gold.mart_daily_demand
            WHERE trip_date >= DATE '2024-01-01' AND trip_date < DATE '2025-01-01'
            """
        ).fetchone()
    result = {
        "fact_rows": fact[0],
        "unique_trips": fact[1],
        "months": fact[2],
        "first_month": fact[3],
        "last_month": fact[4],
        "pickup_type": fact[5],
        "gold_days": gold[0],
        "first_gold_date": str(gold[1]),
        "last_gold_date": str(gold[2]),
    }
    print(json.dumps(result, indent=2))
    if fact[0] != fact[1] or fact[2] != 12:
        raise RuntimeError("Annual fact validation failed")


if __name__ == "__main__":
    main()
