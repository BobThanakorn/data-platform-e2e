from __future__ import annotations

from datetime import date


def months_between(start: str, end: str) -> list[tuple[int, int]]:
    """Return inclusive (year, month) pairs for YYYY-MM boundaries."""
    start_date = date.fromisoformat(f"{start}-01")
    end_date = date.fromisoformat(f"{end}-01")
    if start_date > end_date:
        raise ValueError("start month must not be after end month")
    months: list[tuple[int, int]] = []
    year, month = start_date.year, start_date.month
    while (year, month) <= (end_date.year, end_date.month):
        months.append((year, month))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return months
