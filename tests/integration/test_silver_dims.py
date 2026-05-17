"""Integration tests for silver.conform_dim — conformers + quarantine + dedup grain."""

from __future__ import annotations

import pytest

from src.pipelines.silver.conform_dim import (
    CONFORMERS,
    HARD_RULES,
    NATURAL_KEYS,
    conform_customer,
    conform_geolocation,
    conform_product,
    split_quarantine,
)


# ---------------------------------------------------------------------------
# customer — city to initcap, state to upper
# ---------------------------------------------------------------------------
@pytest.mark.integration
def test_conform_customer_normalizes_city_and_state(spark):
    raw = spark.createDataFrame(
        [("c1", "u1", "14409", "franca", "sp"), ("c2", "u2", "01037", "SAO PAULO", "SP")],
        "customer_id string, customer_unique_id string, customer_zip_code_prefix string, "
        "customer_city string, customer_state string",
    )
    out = {r.customer_id: r for r in conform_customer(raw).collect()}
    assert out["c1"].customer_city == "Franca"
    assert out["c1"].customer_state == "SP"
    assert out["c2"].customer_city == "Sao Paulo"
    assert out["c2"].customer_state == "SP"


# ---------------------------------------------------------------------------
# product — try_cast numerics so unparseable values become null
# ---------------------------------------------------------------------------
@pytest.mark.integration
def test_conform_product_try_casts_numerics(spark):
    raw = spark.createDataFrame(
        [
            ("p1", "perfumaria", "40", "287", "1", "225", "16", "10", "14"),
            # row with a bad weight string should null it, not crash
            ("p2", "perfumaria", "40", "287", "1", "garbage", "16", "10", "14"),
        ],
        "product_id string, product_category_name string, "
        "product_name_lenght string, product_description_lenght string, "
        "product_photos_qty string, product_weight_g string, "
        "product_length_cm string, product_height_cm string, product_width_cm string",
    )
    out = {r.product_id: r for r in conform_product(raw).collect()}
    assert out["p1"].product_weight_g == 225
    assert out["p2"].product_weight_g is None


# ---------------------------------------------------------------------------
# geolocation — collapse multiple rows per zip
# ---------------------------------------------------------------------------
@pytest.mark.integration
def test_conform_geolocation_collapses_to_one_row_per_zip(spark):
    raw = spark.createDataFrame(
        [
            ("01037", -23.55, -46.64, "sao paulo", "sp"),
            ("01037", -23.54, -46.63, "sao paulo", "sp"),
            ("14409", -20.53, -47.40, "franca", "sp"),
        ],
        "geolocation_zip_code_prefix string, geolocation_lat double, geolocation_lng double, "
        "geolocation_city string, geolocation_state string",
    )
    out = {r.geolocation_zip_code_prefix: r for r in conform_geolocation(raw).collect()}
    # Two zips → two rows after collapse
    assert set(out.keys()) == {"01037", "14409"}
    # Lat averaged for duplicates
    assert out["01037"].geolocation_lat == pytest.approx(-23.545, abs=0.001)


# ---------------------------------------------------------------------------
# quarantine — null PK
# ---------------------------------------------------------------------------
@pytest.mark.integration
def test_split_quarantine_isolates_null_pk(spark):
    conformed = spark.createDataFrame(
        [("c1", "u1", "14409", "Franca", "SP", None), (None, "u2", "01037", "Sao Paulo", "SP", None)],
        "customer_id string, customer_unique_id string, customer_zip_code_prefix string, "
        "customer_city string, customer_state string, _silver_processed_ts timestamp",
    )
    valid, quarantined = split_quarantine(conformed, "customer")
    assert valid.count() == 1
    assert quarantined.count() == 1
    assert quarantined.first()._quarantine_reason == "customer_id_null"


# ---------------------------------------------------------------------------
# Module-level contracts
# ---------------------------------------------------------------------------
def test_every_dim_has_conformer_keys_and_hard_rules():
    from src.pipelines.silver.conform_dim import DIM_TABLES

    for t in DIM_TABLES:
        assert t in CONFORMERS, f"{t} missing from CONFORMERS"
        assert t in NATURAL_KEYS, f"{t} missing from NATURAL_KEYS"
        assert t in HARD_RULES, f"{t} missing from HARD_RULES"
        assert len(NATURAL_KEYS[t]) >= 1
