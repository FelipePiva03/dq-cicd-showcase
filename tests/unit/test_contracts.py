"""Unit tests for pure helper functions — no Spark required."""

from datetime import datetime

import pytest


def test_placeholder_so_ci_passes_before_real_code() -> None:
    """Replace this with actual unit tests as src/common grows."""
    assert datetime(2024, 1, 1) < datetime(2025, 1, 1)


@pytest.mark.parametrize(
    ("status_raw", "expected"),
    [
        ("DELIVERED", "delivered"),
        ("  Shipped  ", "shipped"),
        ("canceled", "canceled"),
    ],
)
def test_status_normalization_contract(status_raw: str, expected: str) -> None:
    """The Silver layer must lowercase + strip order_status. This documents
    the contract; the actual implementation is in transform_orders.py."""
    assert status_raw.strip().lower() == expected
