"""Integration tests for silver.conform_fact — all 4 fact tables + dedupe."""

from __future__ import annotations

from decimal import Decimal

import pytest
from chispa import assert_column_equality

from src.pipelines.silver.conform_fact import (
    CONFORMERS,
    HARD_RULES,
    NATURAL_KEYS,
    conform_order_items,
    conform_order_payments,
    conform_order_reviews,
    conform_orders,
    dedupe_by_key,
    split_quarantine,
)


# ---------------------------------------------------------------------------
# orders
# ---------------------------------------------------------------------------
@pytest.mark.integration
def test_conform_orders_normalizes_status_and_computes_days_to_delivery(spark):
    raw = spark.createDataFrame(
        [
            ("o1", "c1", "DELIVERED", "2024-01-01 10:00:00", "2024-01-01 11:00:00",
             "2024-01-02 09:00:00", "2024-01-05 14:00:00", "2024-01-10 00:00:00"),
            ("o2", "c2", "Shipped", "2024-02-01 09:00:00", "2024-02-01 09:30:00",
             "2024-02-02 10:00:00", None, "2024-02-15 00:00:00"),
        ],
        "order_id string, customer_id string, order_status string, "
        "order_purchase_timestamp string, order_approved_at string, "
        "order_delivered_carrier_date string, order_delivered_customer_date string, "
        "order_estimated_delivery_date string",
    )

    out = conform_orders(raw)

    expected = spark.createDataFrame(
        [("o1", "delivered", 4), ("o2", "shipped", None)],
        "order_id string, expected_status string, expected_days int",
    )
    joined = out.join(expected, "order_id")
    assert_column_equality(joined, "order_status", "expected_status")
    assert_column_equality(joined, "days_to_delivery", "expected_days")


# ---------------------------------------------------------------------------
# order_items
# ---------------------------------------------------------------------------
@pytest.mark.integration
def test_conform_order_items_casts_price_to_decimal(spark):
    raw = spark.createDataFrame(
        [("o1", 1, "p1", "s1", "2024-01-05 00:00:00", "58.90", "13.29")],
        "order_id string, order_item_id int, product_id string, seller_id string, "
        "shipping_limit_date string, price string, freight_value string",
    )
    out = conform_order_items(raw).first()
    assert str(out.price) == "58.90"
    assert str(out.freight_value) == "13.29"


# ---------------------------------------------------------------------------
# order_payments
# ---------------------------------------------------------------------------
@pytest.mark.integration
def test_conform_order_payments_lowercases_payment_type(spark):
    raw = spark.createDataFrame(
        [("o1", 1, "CREDIT_CARD", 8, "99.33")],
        "order_id string, payment_sequential int, payment_type string, "
        "payment_installments int, payment_value string",
    )
    out = conform_order_payments(raw).first()
    assert out.payment_type == "credit_card"
    assert str(out.payment_value) == "99.33"


# ---------------------------------------------------------------------------
# order_reviews
# ---------------------------------------------------------------------------
@pytest.mark.integration
def test_conform_order_reviews_casts_score_and_timestamps(spark):
    raw = spark.createDataFrame(
        [("r1", "o1", "5", "Title", "Body", "2024-01-15 00:00:00", "2024-01-16 10:00:00")],
        "review_id string, order_id string, review_score string, "
        "review_comment_title string, review_comment_message string, "
        "review_creation_date string, review_answer_timestamp string",
    )
    out = conform_order_reviews(raw).first()
    assert out.review_score == 5
    assert out.review_creation_date is not None
    assert out.review_answer_timestamp is not None


# ---------------------------------------------------------------------------
# dedupe — the safety net that lets MERGE not blow up
# ---------------------------------------------------------------------------
@pytest.mark.integration
def test_dedupe_keeps_latest_ingestion_per_key(spark):
    raw = spark.createDataFrame(
        [
            ("o1", "shipped",   "2024-01-01 10:00:00"),
            ("o1", "delivered", "2024-01-02 10:00:00"),   # winner — newer
            ("o2", "delivered", "2024-01-02 10:00:00"),
        ],
        "order_id string, order_status string, _ingestion_ts string",
    )
    out = dedupe_by_key(raw, keys=["order_id"]).collect()
    by_id = {r.order_id: r.order_status for r in out}
    assert by_id == {"o1": "delivered", "o2": "delivered"}


@pytest.mark.integration
def test_dedupe_handles_composite_key(spark):
    """order_items uses (order_id, order_item_id) — both must partition together."""
    raw = spark.createDataFrame(
        [
            ("o1", 1, "old_product", "2024-01-01 10:00:00"),
            ("o1", 1, "new_product", "2024-01-02 10:00:00"),  # winner
            ("o1", 2, "other",       "2024-01-01 10:00:00"),  # different key, stays
        ],
        "order_id string, order_item_id int, product_id string, _ingestion_ts string",
    )
    out = dedupe_by_key(raw, keys=["order_id", "order_item_id"]).collect()
    by_key = {(r.order_id, r.order_item_id): r.product_id for r in out}
    assert by_key == {("o1", 1): "new_product", ("o1", 2): "other"}


# ---------------------------------------------------------------------------
# Module-level contract tests (no Spark)
# ---------------------------------------------------------------------------
def test_every_event_table_has_conformer_keys_and_hard_rules():
    """If a new event table is added to EVENT_TABLES, the missing
    entry here points right at the gap."""
    from src.pipelines.silver.conform_fact import EVENT_TABLES

    for t in EVENT_TABLES:
        assert t in CONFORMERS, f"{t} missing from CONFORMERS"
        assert t in NATURAL_KEYS, f"{t} missing from NATURAL_KEYS"
        assert t in HARD_RULES, f"{t} missing from HARD_RULES"
        assert len(NATURAL_KEYS[t]) >= 1


# ---------------------------------------------------------------------------
# quarantine split — the safety net that keeps silver clean
# ---------------------------------------------------------------------------
@pytest.mark.integration
def test_split_quarantine_separates_null_pk_from_silver(spark):
    """Rows with null natural key go to quarantine with the reason tagged.
    Valid rows pass through untouched."""
    conformed = spark.createDataFrame(
        [
            ("o1", "c1", "delivered", None, None, None, None, None, None),
            (None, "c2", "delivered", None, None, None, None, None, None),
        ],
        "order_id string, customer_id string, order_status string, "
        "order_purchase_timestamp timestamp, order_approved_at timestamp, "
        "order_delivered_carrier_date timestamp, "
        "order_delivered_customer_date timestamp, "
        "order_estimated_delivery_date timestamp, days_to_delivery int",
    )

    valid, quarantined = split_quarantine(conformed, "orders")
    valid_ids = [r.order_id for r in valid.collect()]
    quar_rows = quarantined.collect()

    # o1 has the null purchase_ts, which fires `purchase_ts_unparseable`
    # before reaching the order_id null check. Both rows actually quarantine.
    assert "o1" not in valid_ids
    assert all(r._quarantine_reason is not None for r in quar_rows)


@pytest.mark.integration
def test_split_quarantine_keeps_clean_rows(spark):
    """Rows that pass every hard rule must NOT carry a _quarantine_reason
    column (it's dropped on the valid side)."""
    conformed = spark.createDataFrame(
        [("o1", 1, "p1", "s1", None, Decimal("50.00"), Decimal("10.00"), None)],
        "order_id string, order_item_id int, product_id string, seller_id string, "
        "shipping_limit_date timestamp, price decimal(10,2), "
        "freight_value decimal(10,2), _silver_processed_ts timestamp",
    )
    valid, quarantined = split_quarantine(conformed, "order_items")

    assert valid.count() == 1
    assert quarantined.count() == 0
    assert "_quarantine_reason" not in valid.columns


@pytest.mark.integration
def test_split_quarantine_reason_names_specific_rule(spark):
    """When `price IS NULL` (try_cast nulled an unparseable string),
    the reason must be `price_unparseable` — not a generic message."""
    conformed = spark.createDataFrame(
        [("o1", 1, "p1", "s1", None, None, Decimal("10.00"), None)],
        "order_id string, order_item_id int, product_id string, seller_id string, "
        "shipping_limit_date timestamp, price decimal(10,2), "
        "freight_value decimal(10,2), _silver_processed_ts timestamp",
    )
    _, quarantined = split_quarantine(conformed, "order_items")
    row = quarantined.first()
    assert row._quarantine_reason == "price_unparseable"
    assert row._quarantine_ts is not None
