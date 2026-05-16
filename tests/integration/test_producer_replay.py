"""Integration test — producer temporal replay against a tiny fake dataset."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.pipelines.producer.replay_olist import (
    attach_effective_ts,
    replay,
)


def _write_csv(path: Path, header: str, rows: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(header + "\n" + "\n".join(rows) + "\n", encoding="utf-8")


@pytest.fixture
def fake_olist(tmp_path: Path) -> Path:
    """Builds a minimal raw/ dir with 4 event CSVs and known timestamps."""
    raw = tmp_path / "raw"
    _write_csv(
        raw / "olist_orders_dataset.csv",
        "order_id,customer_id,order_status,order_purchase_timestamp,"
        "order_approved_at,order_delivered_carrier_date,"
        "order_delivered_customer_date,order_estimated_delivery_date",
        [
            "o1,c1,delivered,2024-01-02 10:00:00,2024-01-02 11:00:00,"
            "2024-01-03 09:00:00,2024-01-05 12:00:00,2024-01-10 00:00:00",
            "o2,c2,delivered,2024-01-20 09:00:00,2024-01-20 09:30:00,"
            "2024-01-21 10:00:00,2024-01-24 15:00:00,2024-01-30 00:00:00",
        ],
    )
    _write_csv(
        raw / "olist_order_items_dataset.csv",
        "order_id,order_item_id,product_id,seller_id,shipping_limit_date,"
        "price,freight_value",
        [
            "o1,1,p1,s1,2024-01-04 00:00:00,50.00,10.00",
            "o2,1,p2,s2,2024-01-22 00:00:00,80.00,12.00",
        ],
    )
    _write_csv(
        raw / "olist_order_payments_dataset.csv",
        "order_id,payment_sequential,payment_type,payment_installments,payment_value",
        [
            "o1,1,credit_card,2,60.00",
            "o2,1,boleto,1,92.00",
        ],
    )
    _write_csv(
        raw / "olist_order_reviews_dataset.csv",
        "review_id,order_id,review_score,review_comment_title,"
        "review_comment_message,review_creation_date,review_answer_timestamp",
        [
            # Review for o1 arrives 10 days AFTER the purchase -> late event
            "r1,o1,5,,,2024-01-15 00:00:00,2024-01-15 12:00:00",
            "r2,o2,4,,,2024-01-26 00:00:00,2024-01-26 08:00:00",
        ],
    )
    return raw


@pytest.mark.integration
def test_review_uses_review_creation_date_not_purchase_ts(spark, fake_olist):
    """Reviews must bucket by review_creation_date so they appear later
    than the parent order — that's the whole point of using streaming."""
    reviews = (
        spark.read.option("header", "true").option("inferSchema", "true")
        .csv(str(fake_olist / "olist_order_reviews_dataset.csv"))
    )
    orders = (
        spark.read.option("header", "true").option("inferSchema", "true")
        .csv(str(fake_olist / "olist_orders_dataset.csv"))
    )

    keyed = attach_effective_ts("order_reviews", reviews, orders).collect()
    by_review = {r.review_id: r._effective_ts for r in keyed}

    # r1 review was created 2024-01-15, parent order o1 purchased 2024-01-02
    assert str(by_review["r1"]) == "2024-01-15"


@pytest.mark.integration
def test_items_inherit_parent_order_timestamp(spark, fake_olist):
    """order_items must inherit their parent order's purchase timestamp,
    since items don't have their own event time."""
    items = (
        spark.read.option("header", "true").option("inferSchema", "true")
        .csv(str(fake_olist / "olist_order_items_dataset.csv"))
    )
    orders = (
        spark.read.option("header", "true").option("inferSchema", "true")
        .csv(str(fake_olist / "olist_orders_dataset.csv"))
    )

    keyed = attach_effective_ts("order_items", items, orders).collect()
    by_order = {r.order_id: r._effective_ts for r in keyed}

    assert str(by_order["o1"]) == "2024-01-02"
    assert str(by_order["o2"]) == "2024-01-20"


@pytest.mark.integration
def test_replay_produces_batches_per_table(spark, fake_olist, tmp_path):
    landing = tmp_path / "landing"
    totals = replay(
        spark,
        source_root=str(fake_olist),
        landing_root=str(landing),
        tables=["orders", "order_items", "order_payments", "order_reviews"],
        window_days=7,
    )

    # All 4 tables should have written rows
    assert totals == {
        "orders": 2,
        "order_items": 2,
        "order_payments": 2,
        "order_reviews": 2,
    }

    # Late-arriving review proof: o1 (purchased 2024-01-02) and r1
    # (created 2024-01-15) land in DIFFERENT batch directories.
    orders_batches = {p.name for p in (landing / "orders").iterdir()}
    reviews_batches = {p.name for p in (landing / "order_reviews").iterdir()}

    assert orders_batches.isdisjoint(reviews_batches), (
        f"orders and reviews shared batches {orders_batches & reviews_batches} "
        "— late-arriving semantics broken"
    )
