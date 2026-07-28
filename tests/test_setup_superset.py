from __future__ import annotations

import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_setup_superset():
    module_path = ROOT / "scripts" / "setup_superset.py"
    spec = importlib.util.spec_from_file_location("setup_superset", module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_chart_specs_include_kpi_heatmap_and_quality_charts() -> None:
    mod = load_setup_superset()
    datasets = {
        "mart_daily_demand": 1,
        "mart_zone_performance": 2,
        "mart_hourly_pattern": 3,
        "pipeline_runs": 4,
    }
    specs = mod.chart_specs(datasets)

    assert len(specs) >= 8
    viz_types = {spec["viz_type"] for spec in specs}
    names = {spec["slice_name"] for spec in specs}

    assert "big_number_total" in viz_types
    assert "heatmap_v2" in viz_types
    assert "echarts_timeseries_line" in viz_types
    assert "Total Trips" in names
    assert "Weekday Hour Demand Heatmap" in names
    assert "Data Quality Rejection Rate" in names


def test_build_query_context_is_valid_json_with_metrics() -> None:
    mod = load_setup_superset()
    spec = mod.chart_specs(
        {
            "mart_daily_demand": 1,
            "mart_zone_performance": 2,
            "mart_hourly_pattern": 3,
            "pipeline_runs": 4,
        }
    )[4]
    context = json.loads(mod.build_query_context(spec))

    assert context["datasource"]["id"] == spec["datasource_id"]
    assert context["queries"][0]["metrics"]


def test_dashboard_position_covers_all_charts() -> None:
    mod = load_setup_superset()
    chart_ids = list(range(1, 9))
    layout = json.loads(mod.dashboard_position(chart_ids))

    placed = {
        node["meta"]["chartId"]
        for node in layout.values()
        if isinstance(node, dict) and node.get("type") == "CHART"
    }
    assert placed == set(chart_ids)
