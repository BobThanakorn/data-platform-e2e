from __future__ import annotations

import json
import os
import time
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = os.getenv("SUPERSET_URL", "http://localhost:8088").rstrip("/")
ADMIN_USER = os.getenv("SUPERSET_ADMIN_USER", "admin")
ADMIN_PASSWORD = os.getenv("SUPERSET_ADMIN_PASSWORD", "admin")
DATABASE_NAME = "NYC Taxi Analytics"
DASHBOARD_TITLE = "NYC Taxi Analytics"
DASHBOARD_SLUG = "nyc-taxi-analytics"


def request_json(
    session: requests.Session,
    method: str,
    path: str,
    *,
    headers: dict[str, str] | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    response = session.request(
        method,
        f"{BASE_URL}{path}",
        headers=headers,
        timeout=60,
        **kwargs,
    )
    if not response.ok:
        raise RuntimeError(
            f"Superset API {method} {path} failed ({response.status_code}): {response.text}"
        )
    return response.json()


def wait_for_superset(session: requests.Session) -> None:
    for attempt in range(1, 61):
        try:
            response = session.get(f"{BASE_URL}/health", timeout=5)
            if response.ok:
                return
        except requests.RequestException:
            pass
        if attempt == 60:
            raise TimeoutError("Superset did not become healthy within five minutes")
        time.sleep(5)


def result_id(payload: dict[str, Any]) -> int:
    for key in ("id", "table_id"):
        if key in payload and payload[key] is not None:
            return int(payload[key])
    result = payload.get("result")
    if isinstance(result, dict):
        return result_id(result)
    raise KeyError(f"Superset response does not contain a resource ID: {payload}")


def list_resources(
    session: requests.Session,
    path: str,
    headers: dict[str, str],
) -> list[dict[str, Any]]:
    payload = request_json(
        session,
        "GET",
        path,
        headers=headers,
        params={"q": "(page:0,page_size:100)"},
    )
    result = payload.get("result", [])
    if not isinstance(result, list):
        raise TypeError(f"Unexpected Superset list response: {payload}")
    return result


def find_named(
    resources: list[dict[str, Any]], field: str, value: str
) -> dict[str, Any] | None:
    return next((item for item in resources if item.get(field) == value), None)


def create_database(
    session: requests.Session, headers: dict[str, str]
) -> int:
    databases = list_resources(session, "/api/v1/database/", headers)
    existing = find_named(databases, "database_name", DATABASE_NAME)
    if existing:
        return int(existing["id"])

    user = os.getenv("POSTGRES_USER", "platform")
    password = os.getenv("POSTGRES_PASSWORD", "platform_dev_password")
    database = os.getenv("POSTGRES_DB", "analytics")
    uri = f"postgresql+psycopg2://{user}:{password}@postgres:5432/{database}"
    payload = {
        "database_name": DATABASE_NAME,
        "configuration_method": "sqlalchemy_form",
        "sqlalchemy_uri": uri,
        "engine": "postgresql",
        "driver": "psycopg2",
        "expose_in_sqllab": True,
        "allow_ctas": False,
        "allow_cvas": False,
        "allow_dml": False,
        "allow_file_upload": False,
    }
    return result_id(
        request_json(
            session,
            "POST",
            "/api/v1/database/",
            headers=headers,
            json=payload,
        )
    )


def create_datasets(
    session: requests.Session,
    headers: dict[str, str],
    database_id: int,
) -> dict[str, int]:
    datasets: dict[str, int] = {}
    for schema, table in [
        ("gold", "mart_daily_demand"),
        ("gold", "mart_zone_performance"),
        ("gold", "mart_hourly_pattern"),
        ("audit", "pipeline_runs"),
    ]:
        payload = request_json(
            session,
            "POST",
            "/api/v1/dataset/get_or_create/",
            headers=headers,
            json={"database_id": database_id, "schema": schema, "table_name": table},
        )
        datasets[table] = result_id(payload)
    return datasets


def create_dashboard(session: requests.Session, headers: dict[str, str]) -> int:
    dashboards = list_resources(session, "/api/v1/dashboard/", headers)
    existing = find_named(dashboards, "slug", DASHBOARD_SLUG)
    if not existing:
        existing = find_named(dashboards, "slug", "nyc-taxi-january-2024")
    if existing:
        return int(existing["id"])
    payload = {
        "dashboard_title": DASHBOARD_TITLE,
        "slug": DASHBOARD_SLUG,
        "published": False,
        "json_metadata": json.dumps(
            {"native_filter_configuration": [], "chart_configuration": {}}
        ),
        "position_json": "{}",
    }
    return result_id(
        request_json(
            session,
            "POST",
            "/api/v1/dashboard/",
            headers=headers,
            json=payload,
        )
    )


def chart_specs(datasets: dict[str, int]) -> list[dict[str, Any]]:
    daily = datasets["mart_daily_demand"]
    zones = datasets["mart_zone_performance"]
    hourly = datasets["mart_hourly_pattern"]
    audit = datasets["pipeline_runs"]
    def simple_metric(column: str, aggregate: str, label: str) -> dict[str, Any]:
        return {
            "expressionType": "SIMPLE",
            "column": {"column_name": column},
            "aggregate": aggregate,
            "label": label,
        }

    def kpi(name: str, column: str, aggregate: str, label: str) -> dict[str, Any]:
        return {
            "slice_name": name,
            "datasource_id": daily,
            "viz_type": "big_number_total",
            "params": {
                "viz_type": "big_number_total",
                "datasource": f"{daily}__table",
                "metric": simple_metric(column, aggregate, label),
                "adhoc_filters": [],
                "time_range": "No filter",
                "y_axis_format": "SMART_NUMBER",
            },
        }

    return [
        kpi("Total Trips", "trip_count", "SUM", "Trips"),
        kpi("Total Revenue", "revenue_amount", "SUM", "Revenue"),
        kpi("Average Fare", "average_fare_amount", "AVG", "Average fare"),
        kpi("Average Tip Rate", "tip_rate_pct", "AVG", "Tip rate %"),
        {
            "slice_name": "Daily Taxi Demand",
            "datasource_id": daily,
            "viz_type": "echarts_timeseries_line",
            "params": {
                "viz_type": "echarts_timeseries_line",
                "datasource": f"{daily}__table",
                "x_axis": "trip_date",
                "granularity_sqla": "trip_date",
                "time_grain_sqla": "P1D",
                "time_range": "No filter",
                "metrics": [
                    simple_metric("trip_count", "SUM", "Trips"),
                    simple_metric("revenue_amount", "SUM", "Revenue"),
                ],
                "groupby": [],
                "adhoc_filters": [],
                "row_limit": 10000,
                "show_legend": True,
            },
        },
        {
            "slice_name": "Top Taxi Zones",
            "datasource_id": zones,
            "viz_type": "table",
            "params": {
                "viz_type": "table",
                "datasource": f"{zones}__table",
                "query_mode": "aggregate",
                "groupby": ["borough", "zone_name"],
                "metrics": [
                    simple_metric("trip_count", "SUM", "Trips"),
                    simple_metric("revenue_amount", "SUM", "Revenue"),
                ],
                "adhoc_filters": [],
                "row_limit": 25,
                "order_desc": True,
            },
        },
        {
            "slice_name": "Weekday Hour Demand Heatmap",
            "datasource_id": hourly,
            "viz_type": "heatmap_v2",
            "params": {
                "viz_type": "heatmap_v2",
                "datasource": f"{hourly}__table",
                "query_mode": "aggregate",
                "all_columns_x": "pickup_hour",
                "all_columns_y": "weekday_name",
                "metric": simple_metric("trip_count", "SUM", "Trips"),
                "adhoc_filters": [],
                "row_limit": 10000,
                "normalize_across": "heatmap",
                "linear_color_scheme": "blue_white_yellow",
            },
        },
        {
            "slice_name": "Data Quality Rejection Rate",
            "datasource_id": audit,
            "viz_type": "big_number_total",
            "params": {
                "viz_type": "big_number_total",
                "datasource": f"{audit}__table",
                "metric": {
                    "expressionType": "SQL",
                    "sqlExpression": (
                        "100.0 * SUM(rejected_rows) / NULLIF(SUM(bronze_rows), 0)"
                    ),
                    "label": "Rejected %",
                },
                "adhoc_filters": [],
                "time_range": "No filter",
                "y_axis_format": ".2f",
            },
        },
    ]


def build_query_context(spec: dict[str, Any]) -> str:
    params = spec["params"]
    metrics = params.get("metrics") or ([params["metric"]] if params.get("metric") else [])
    columns = list(params.get("groupby", []))
    for key in ("all_columns_x", "all_columns_y"):
        if params.get(key):
            columns.append(params[key])
    query = {
        "filters": [],
        "extras": {
            "time_grain_sqla": params.get("time_grain_sqla"),
            "having": "",
            "where": "",
        },
        "time_range": params.get("time_range", "No filter"),
        "applied_time_extras": {},
        "columns": columns,
        "metrics": metrics,
        "orderby": [],
        "annotation_layers": [],
        "row_limit": params.get("row_limit", 10000),
        "series_limit": 0,
        "order_desc": params.get("order_desc", True),
        "url_params": {},
        "custom_params": {},
        "custom_form_data": {},
    }
    if params.get("x_axis") or params.get("granularity_sqla"):
        query["granularity"] = params.get("x_axis") or params.get("granularity_sqla")
        query["is_timeseries"] = True
    context = {
        "datasource": {"id": spec["datasource_id"], "type": "table"},
        "force": False,
        "queries": [query],
        "form_data": params,
        "result_format": "json",
        "result_type": "full",
    }
    return json.dumps(context)


def create_charts(
    session: requests.Session,
    headers: dict[str, str],
    dashboard_id: int,
    datasets: dict[str, int],
) -> list[int]:
    existing = {
        item["slice_name"]: int(item["id"])
        for item in list_resources(session, "/api/v1/chart/", headers)
    }
    legacy_names = {
        "Weekday Hour Demand Heatmap": "Hourly Demand Pattern",
        "Data Quality Rejection Rate": "Pipeline Run History",
    }
    chart_ids: list[int] = []
    for spec in chart_specs(datasets):
        name = spec["slice_name"]
        payload = {
            **spec,
            "datasource_type": "table",
            "dashboards": [dashboard_id],
            "params": json.dumps(spec["params"]),
            "query_context": build_query_context(spec),
        }
        existing_name = name if name in existing else legacy_names.get(name)
        if existing_name in existing:
            chart_id = existing[existing_name]
            request_json(
                session,
                "PUT",
                f"/api/v1/chart/{chart_id}",
                headers=headers,
                json=payload,
            )
        else:
            chart_id = result_id(
                request_json(
                    session,
                    "POST",
                    "/api/v1/chart/",
                    headers=headers,
                    json=payload,
                )
            )
        chart_ids.append(chart_id)
    return chart_ids


def dashboard_position(chart_ids: list[int]) -> str:
    row_ids = [f"ROW-{index + 1}" for index in range((len(chart_ids) + 1) // 2)]
    position: dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
        "GRID_ID": {
            "id": "GRID_ID",
            "type": "GRID",
            "parents": ["ROOT_ID"],
            "children": row_ids,
        },
    }
    for row_index, row_id in enumerate(row_ids):
        row_charts = chart_ids[row_index * 2 : row_index * 2 + 2]
        position[row_id] = {
            "id": row_id,
            "type": "ROW",
            "parents": ["ROOT_ID", "GRID_ID"],
            "children": [f"CHART-{chart_id}" for chart_id in row_charts],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for chart_id in row_charts:
            position[f"CHART-{chart_id}"] = {
                "id": f"CHART-{chart_id}",
                "type": "CHART",
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "children": [],
                "meta": {"chartId": chart_id, "width": 6, "height": 50},
            }
    return json.dumps(position)


def main() -> None:
    session = requests.Session()
    wait_for_superset(session)
    login = request_json(
        session,
        "POST",
        "/api/v1/security/login",
        json={
            "username": ADMIN_USER,
            "password": ADMIN_PASSWORD,
            "provider": "db",
            "refresh": True,
        },
    )
    access_token = login["access_token"]
    auth_headers = {"Authorization": f"Bearer {access_token}"}
    csrf = request_json(
        session,
        "GET",
        "/api/v1/security/csrf_token/",
        headers=auth_headers,
    )["result"]
    mutation_headers = {
        **auth_headers,
        "X-CSRFToken": csrf,
        "Referer": f"{BASE_URL}/",
        "Content-Type": "application/json",
    }

    database_id = create_database(session, mutation_headers)
    datasets = create_datasets(session, mutation_headers, database_id)
    dashboard_id = create_dashboard(session, mutation_headers)
    chart_ids = create_charts(session, mutation_headers, dashboard_id, datasets)
    request_json(
        session,
        "PUT",
        f"/api/v1/dashboard/{dashboard_id}",
        headers=mutation_headers,
        json={
            "dashboard_title": DASHBOARD_TITLE,
            "slug": DASHBOARD_SLUG,
            "published": True,
            "position_json": dashboard_position(chart_ids),
            "json_metadata": json.dumps(
                {"native_filter_configuration": [], "chart_configuration": {}}
            ),
        },
    )
    print(
        json.dumps(
            {
                "database_id": database_id,
                "datasets": datasets,
                "dashboard_id": dashboard_id,
                "chart_ids": chart_ids,
                "dashboard_url": f"{BASE_URL}/superset/dashboard/{DASHBOARD_SLUG}/",
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
