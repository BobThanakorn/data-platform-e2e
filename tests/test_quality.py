from pathlib import Path

import pytest

from data_platform.quality import validate_with_great_expectations


def metrics(rejected_rate_pct: float = 1.0) -> dict[str, object]:
    return {
        "bronze_rows": 100,
        "silver_rows": 99,
        "rejected_rate_pct": rejected_rate_pct,
        "freshness_sla_met": True,
    }


def test_great_expectations_accepts_healthy_metrics(tmp_path: Path) -> None:
    result = validate_with_great_expectations(metrics(), tmp_path / "gx.json")
    assert result["success"] is True
    assert (tmp_path / "gx.json").exists()


def test_great_expectations_rejects_high_quarantine_rate(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="quality gate failed"):
        validate_with_great_expectations(metrics(6.0), tmp_path / "gx.json")
