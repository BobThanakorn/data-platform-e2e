import pytest

from data_platform.backfill import months_between


def test_months_between_includes_boundaries() -> None:
    assert months_between("2024-11", "2025-02") == [
        (2024, 11),
        (2024, 12),
        (2025, 1),
        (2025, 2),
    ]


def test_months_between_rejects_reverse_range() -> None:
    with pytest.raises(ValueError):
        months_between("2025-01", "2024-12")
