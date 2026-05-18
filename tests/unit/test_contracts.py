"""Unit tests for data contracts and their adapters."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.dq.contracts import (
    list_contracts,
    load_contract,
    to_gx_suite,
    to_hard_rules_case,
    to_soda_yaml,
)

CONTRACTS_DIR = Path("contracts")
VALID_SEVERITIES = {"critical", "warning"}

ALL_LAYERS = ("silver", "gold")
FACT_TABLES = ("fact_orders", "fact_order_items", "fact_order_payments", "fact_order_reviews")
DIM_TABLES = ("dim_customer", "dim_product", "dim_seller", "dim_geolocation", "dim_category")
GOLD_TABLES = ("daily_orders", "delivery_performance")

ALL_SILVER = FACT_TABLES + DIM_TABLES
ALL_LAYER_TABLE = [("silver", t) for t in ALL_SILVER] + [("gold", t) for t in GOLD_TABLES]


# ---------- presence + parse ------------------------------------------------


@pytest.mark.parametrize("layer", ALL_LAYERS)
def test_layer_dir_exists(layer):
    assert (CONTRACTS_DIR / layer).is_dir()


@pytest.mark.parametrize("table", ALL_SILVER)
def test_silver_contract_loads(table):
    c = load_contract("silver", table)
    assert c.table == table
    assert c.catalog_schema == "silver"
    assert c.natural_key, f"silver/{table} must declare a natural_key"


@pytest.mark.parametrize("table", GOLD_TABLES)
def test_gold_contract_loads(table):
    c = load_contract("gold", table)
    assert c.table == table
    assert c.catalog_schema == "gold"


def test_list_contracts_finds_all_silver():
    contracts = list_contracts("silver")
    assert len(contracts) == len(ALL_SILVER)


# ---------- schema/quality structural invariants ----------------------------


@pytest.mark.parametrize("layer,table", ALL_LAYER_TABLE)
def test_every_expectation_has_severity_and_rationale(layer, table):
    c = load_contract(layer, table)
    for exp in c.expectations:
        assert exp.severity in VALID_SEVERITIES, f"{layer}/{table}: {exp.id} invalid severity"
        assert exp.rationale.strip(), f"{layer}/{table}: {exp.id} missing rationale"


@pytest.mark.parametrize("table", ALL_SILVER)
def test_silver_contracts_declare_hard_rules(table):
    """Silver contracts must declare at least one hard rule (PK not null at minimum)."""
    c = load_contract("silver", table)
    assert c.hard_rules, f"silver/{table} must declare at least one hard_rule"


@pytest.mark.parametrize("table", GOLD_TABLES)
def test_gold_contracts_have_no_hard_rules(table):
    """Gold marts read from already-cleaned silver — no quarantine flow."""
    c = load_contract("gold", table)
    assert not c.hard_rules, f"gold/{table} must NOT declare hard_rules"


# ---------- adapters --------------------------------------------------------


@pytest.mark.parametrize("table", ALL_SILVER)
def test_to_hard_rules_case_returns_valid_sql_shape(table):
    """The CASE expression must contain every reason+condition from the contract."""
    c = load_contract("silver", table)
    case = to_hard_rules_case(c)
    assert "CASE" in case and "ELSE NULL" in case and "END" in case
    for hr in c.hard_rules:
        assert hr.condition in case
        assert f"'{hr.reason}'" in case


@pytest.mark.parametrize("table", FACT_TABLES)
def test_to_gx_suite_round_trip(table):
    """to_gx_suite produces a suite with one Expectation per contract entry."""
    c = load_contract("silver", table)
    suite = to_gx_suite(c)
    assert suite.name == f"silver_{table}_suite"
    assert len(suite.expectations) == len(c.expectations)


def test_to_soda_yaml_emits_check_blocks_per_table():
    contracts = [load_contract("silver", t) for t in DIM_TABLES]
    out = to_soda_yaml(contracts)
    for t in DIM_TABLES:
        assert f"checks for {t}:" in out


def test_to_soda_yaml_tiered_severity():
    """warning expectations must add `warn: when ...`; critical must not."""
    c = load_contract("silver", "dim_customer")
    out = to_soda_yaml([c])
    # dim_customer has critical (unique, row_count) + warning (state in set)
    assert "warn: when" in out, "warning expectations should emit `warn: when`"
