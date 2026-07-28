from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]

CORE_SERVICES = {
    "postgres",
    "minio",
    "airflow-init",
    "airflow-webserver",
    "airflow-scheduler",
    "superset",
    "superset-bootstrap",
}

EXTENSION_PROFILES = {"observability", "lineage", "streaming", "spark"}


def load_compose() -> dict:
    compose_path = ROOT / "compose.yaml"
    return yaml.safe_load(compose_path.read_text(encoding="utf-8"))


def test_core_services_start_without_profiles() -> None:
    services = load_compose()["services"]
    for name in CORE_SERVICES:
        assert name in services
        profiles = services[name].get("profiles")
        assert profiles is None, f"{name} must not require a compose profile"


def test_extension_services_are_profile_gated() -> None:
    services = load_compose()["services"]
    extension_services = set(services) - CORE_SERVICES
    assert extension_services, "expected optional extension services in compose.yaml"

    for name, config in services.items():
        if name in CORE_SERVICES:
            continue
        profiles = set(config.get("profiles", []))
        assert profiles, f"{name} must declare compose profiles"
        assert profiles & EXTENSION_PROFILES, f"{name} uses unknown profile(s): {profiles}"


def test_core_services_do_not_depend_on_extensions() -> None:
    services = load_compose()["services"]
    extension_names = set(services) - CORE_SERVICES

    for name in CORE_SERVICES:
        depends_on = services[name].get("depends_on", {})
        if isinstance(depends_on, list):
            deps = set(depends_on)
        else:
            deps = set(depends_on)
        overlap = deps & extension_names
        assert not overlap, f"{name} must not depend on extension services: {overlap}"
