from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_dbt_project_declares_three_medallion_model_groups() -> None:
    project = yaml.safe_load((ROOT / "dbt" / "dbt_project.yml").read_text(encoding="utf-8"))
    models = project["models"]["nyc_taxi_medallion"]
    assert {"staging", "silver", "gold"}.issubset(models)


def test_all_expected_gold_models_exist() -> None:
    gold = ROOT / "dbt" / "models" / "gold"
    names = {path.stem for path in gold.glob("*.sql")}
    assert names == {
        "mart_daily_demand",
        "mart_zone_performance",
        "mart_hourly_pattern",
    }


def test_incremental_fact_model_exists() -> None:
    source = (ROOT / "dbt" / "models" / "silver" / "fact_trips.sql").read_text(
        encoding="utf-8"
    )
    assert "materialized='incremental'" in source
    assert "unique_key='trip_id'" in source


def test_airflow_dag_has_no_heavy_top_level_pipeline_imports() -> None:
    dag_source = (ROOT / "dags" / "nyc_taxi_medallion.py").read_text(encoding="utf-8")
    assert "from data_platform.ingest import ingest_month" in dag_source
    assert dag_source.index("def ingest(") < dag_source.index(
        "from data_platform.ingest import ingest_month"
    )
    assert "@task.short_circuit" in dag_source
    assert "source_is_available" in dag_source
