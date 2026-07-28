-- KPI cards: latest loaded day
SELECT
    trip_date,
    trip_count,
    revenue_amount,
    average_fare_amount,
    tip_rate_pct
FROM gold.mart_daily_demand
ORDER BY trip_date DESC
LIMIT 1;

-- Time-series chart: daily trips and revenue
SELECT
    trip_date,
    trip_count,
    revenue_amount
FROM gold.mart_daily_demand
ORDER BY trip_date;

-- Bar chart: top pickup zones by revenue
SELECT
    zone_name,
    borough,
    SUM(trip_count) AS trip_count,
    SUM(revenue_amount) AS revenue_amount
FROM gold.mart_zone_performance
GROUP BY zone_name, borough
ORDER BY revenue_amount DESC
LIMIT 20;

-- Heatmap: demand by weekday and pickup hour
SELECT
    weekday_name,
    weekday_number,
    pickup_hour,
    SUM(trip_count) AS trip_count
FROM gold.mart_hourly_pattern
GROUP BY weekday_name, weekday_number, pickup_hour
ORDER BY weekday_number, pickup_hour;

-- Data-quality panel
SELECT
    dataset_month,
    bronze_rows,
    silver_rows,
    rejected_rows,
    ROUND(100.0 * rejected_rows / NULLIF(bronze_rows, 0), 4) AS rejected_rate_pct,
    completed_at
FROM audit.pipeline_runs
WHERE status = 'success'
ORDER BY completed_at DESC;
