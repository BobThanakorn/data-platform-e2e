from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402


def test_nyc_taxi_dag_loads_without_errors() -> None:
    dag_folder = Path(__file__).parents[1] / "dags"
    dag_bag = DagBag(dag_folder=str(dag_folder), include_examples=False)
    assert dag_bag.import_errors == {}
    dag = dag_bag.get_dag("nyc_taxi_medallion")
    assert dag is not None
    assert set(dag.task_ids) == {
        "resolve_month",
        "source_is_available",
        "ingest",
        "dbt_build",
        "publish",
        "summarize",
    }
