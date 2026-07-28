from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import pyarrow.parquet as pq
from kafka import KafkaProducer


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay Bronze taxi trips to Redpanda.")
    parser.add_argument("--source", required=True)
    parser.add_argument("--bootstrap-servers", default="redpanda:9092")
    parser.add_argument("--topic", default="nyc-taxi-trips")
    parser.add_argument("--limit", type=int, default=10000)
    parser.add_argument("--events-per-second", type=int, default=1000)
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda value: json.dumps(value, default=str).encode(),
        acks="all",
    )
    sent = 0
    interval = 1 / max(args.events_per_second, 1)
    columns = [
        "tpep_pickup_datetime",
        "PULocationID",
        "passenger_count",
        "trip_distance",
        "total_amount",
    ]
    parquet = pq.ParquetFile(Path(args.source))
    for batch in parquet.iter_batches(batch_size=1000, columns=columns):
        for event in batch.to_pylist():
            producer.send(args.topic, event)
            sent += 1
            if sent >= args.limit:
                producer.flush()
                print(f"Replayed {sent} events to {args.topic}")
                return
            time.sleep(interval)
    producer.flush()
    print(f"Replayed {sent} events to {args.topic}")


if __name__ == "__main__":
    main()
