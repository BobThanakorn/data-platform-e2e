from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from data_platform.ingest import sha256_file, source_metadata, validate_trip_file


def _required_table() -> pa.Table:
    return pa.table(
        {
            "VendorID": [1],
            "tpep_pickup_datetime": [datetime(2024, 1, 1, 10, 0)],
            "tpep_dropoff_datetime": [datetime(2024, 1, 1, 10, 15)],
            "passenger_count": [1],
            "trip_distance": [2.5],
            "PULocationID": [100],
            "DOLocationID": [200],
            "payment_type": [1],
            "fare_amount": [12.5],
            "tip_amount": [2.5],
            "total_amount": [18.0],
        }
    )


def test_validate_trip_file_accepts_required_schema(tmp_path: Path) -> None:
    source = tmp_path / "trips.parquet"
    pq.write_table(_required_table(), source)

    rows, columns = validate_trip_file(source)

    assert rows == 1
    assert "total_amount" in columns
    assert len(sha256_file(source)) == 64


def test_validate_trip_file_rejects_missing_columns(tmp_path: Path) -> None:
    source = tmp_path / "invalid.parquet"
    pq.write_table(pa.table({"VendorID": [1]}), source)

    with pytest.raises(ValueError, match="missing required columns"):
        validate_trip_file(source)


def test_source_metadata_marks_unpublished_month_unavailable(monkeypatch) -> None:
    class Response:
        status_code = 403
        headers = {}

    monkeypatch.setattr("data_platform.ingest.requests.head", lambda *args, **kwargs: Response())
    assert source_metadata(2024, 1)["available"] is False
