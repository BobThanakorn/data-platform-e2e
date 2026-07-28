CREATE DATABASE analytics;
CREATE DATABASE marquez;

\connect analytics

CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    run_id text PRIMARY KEY,
    dataset_month text NOT NULL,
    status text NOT NULL,
    bronze_rows bigint,
    silver_rows bigint,
    rejected_rows bigint,
    started_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    details jsonb NOT NULL DEFAULT '{}'::jsonb
);
