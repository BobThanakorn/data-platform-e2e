from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as functions


def main() -> None:
    parser = argparse.ArgumentParser(description="Distributed monthly taxi summary experiment.")
    parser.add_argument("--source", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("nyc-taxi-monthly-summary").getOrCreate()
    trips = spark.read.parquet(args.source)
    summary = (
        trips.withColumn("trip_date", functions.to_date("tpep_pickup_datetime"))
        .groupBy("trip_date")
        .agg(
            functions.count("*").alias("trip_count"),
            functions.round(functions.sum("total_amount"), 2).alias("revenue_amount"),
            functions.round(functions.avg("trip_distance"), 2).alias(
                "average_distance_miles"
            ),
        )
        .orderBy("trip_date")
    )
    summary.write.mode("overwrite").parquet(args.output)
    print(f"Spark wrote {summary.count()} daily rows to {args.output}")
    spark.stop()


if __name__ == "__main__":
    main()
