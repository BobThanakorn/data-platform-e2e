from __future__ import annotations

import json
import os

import requests
from dotenv import load_dotenv

load_dotenv()


def main() -> None:
    base_url = os.getenv("SUPERSET_URL", "http://localhost:8088").rstrip("/")
    session = requests.Session()
    token = session.post(
        f"{base_url}/api/v1/security/login",
        json={
            "username": os.getenv("SUPERSET_ADMIN_USER", "admin"),
            "password": os.getenv("SUPERSET_ADMIN_PASSWORD", "admin"),
            "provider": "db",
        },
        timeout=30,
    )
    token.raise_for_status()
    headers = {"Authorization": f"Bearer {token.json()['access_token']}"}
    charts_response = session.get(
        f"{base_url}/api/v1/chart/",
        headers=headers,
        params={"q": "(page:0,page_size:100)"},
        timeout=30,
    )
    charts_response.raise_for_status()
    charts = charts_response.json()["result"]
    failures: list[str] = []
    verified: list[dict[str, object]] = []
    for chart in charts:
        detail = session.get(
            f"{base_url}/api/v1/chart/{chart['id']}",
            headers=headers,
            timeout=30,
        )
        detail.raise_for_status()
        query_context = detail.json()["result"]["query_context"]
        response = session.post(
            f"{base_url}/api/v1/chart/data",
            headers={**headers, "Content-Type": "application/json"},
            json=json.loads(query_context),
            timeout=120,
        )
        if not response.ok:
            failures.append(f"{chart['slice_name']}: {response.status_code} {response.text}")
            continue
        payload = response.json()
        rows = sum(len(item.get("data", [])) for item in payload.get("result", []))
        verified.append({"id": chart["id"], "name": chart["slice_name"], "rows": rows})

    print(json.dumps({"verified": verified, "failures": failures}, indent=2))
    if failures:
        raise RuntimeError("One or more Superset chart queries failed")


if __name__ == "__main__":
    main()
