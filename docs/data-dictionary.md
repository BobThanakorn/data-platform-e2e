# Data dictionary

## `silver.fact_trips`

Grain: one accepted, deduplicated taxi trip.

| Column | Meaning |
| --- | --- |
| `trip_id` | Deterministic SHA-256 identity |
| `dataset_month` | Source partition in `YYYY-MM` |
| `pickup_at`, `dropoff_at` | UTC timestamps converted from America/New_York |
| `pickup_zone_id`, `dropoff_zone_id` | NYC TLC taxi-zone keys |
| `passenger_count` | Reported passengers |
| `trip_distance_miles` | Reported distance |
| `fare_amount`, `tip_amount`, `total_amount` | USD monetary measures |
| `duration_minutes` | Drop-off minus pickup |
| `average_speed_mph` | Distance divided by duration |
| `quality_status` | `accepted` for fact rows |

## `silver.silver_quarantine`

Same source grain as the fact, containing rejected rows. `quality_status` explains whether the
record has missing timestamps/zones, invalid duration, out-of-range distance, or amount.

## `silver.dim_zone`

Grain: one NYC TLC taxi zone. Includes borough, zone name and service zone.

## `gold.mart_daily_demand`

Grain: one pickup date. Measures trips, passengers, distance, revenue, average fare/duration and
tip rate.

## `gold.mart_zone_performance`

Grain: one pickup date and pickup zone. Measures trips, revenue, average fare/duration and speed.

## `gold.mart_hourly_pattern`

Grain: one dataset month, weekday and pickup hour. Measures trips, fare, duration and tip rate.

## `audit.pipeline_runs`

Grain: one Airflow pipeline run. Records Bronze/Silver/rejected counts, timestamps, status and JSON
details including freshness SLA, Great Expectations result, export manifests and volume anomaly.

## `streaming.trip_events_minute`

Grain: one event minute and pickup zone. Stores replayed trip count and revenue from Redpanda.
