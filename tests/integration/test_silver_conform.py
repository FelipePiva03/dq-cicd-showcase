"""Integration test — Silver conform() with a real local Spark session."""

from __future__ import annotations

import pytest
from chispa import assert_column_equality

from src.pipelines.silver.transform_orders import conform


@pytest.mark.integration
def test_conform_lowercases_status_and_computes_days_to_delivery(spark) -> None:
    raw = spark.createDataFrame(
        [
            ("o1", "c1", "DELIVERED", "2024-01-01 10:00:00", "2024-01-01 11:00:00", "2024-01-05 14:00:00"),
            ("o2", "c2", "Shipped", "2024-02-01 09:00:00", "2024-02-01 09:30:00", None),
        ],
        "order_id string, customer_id string, order_status string, "
        "order_purchase_timestamp string, order_approved_at string, "
        "order_delivered_customer_date string",
    )

    out = conform(raw)

    # status is lowercased
    expected_status = spark.createDataFrame(
        [("o1", "delivered"), ("o2", "shipped")], "order_id string, expected string"
    )
    joined = out.join(expected_status, "order_id")
    assert_column_equality(joined, "order_status", "expected")

    # days_to_delivery computed when delivery date is present, null otherwise
    o1 = out.filter(out.order_id == "o1").first()
    o2 = out.filter(out.order_id == "o2").first()
    assert o1.days_to_delivery == 4
    assert o2.days_to_delivery is None
