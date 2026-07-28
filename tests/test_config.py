import pytest

from data_platform.config import trip_url, validate_month


def test_trip_url_is_partition_specific() -> None:
    assert trip_url(2024, 1).endswith("yellow_tripdata_2024-01.parquet")
    assert trip_url(2024, 12).endswith("yellow_tripdata_2024-12.parquet")


@pytest.mark.parametrize("year,month", [(2024, 0), (2024, 13), (2008, 1)])
def test_validate_month_rejects_invalid_values(year: int, month: int) -> None:
    with pytest.raises(ValueError):
        validate_month(year, month)
