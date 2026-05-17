"""Unit tests for bronze.load_dim path conventions."""

from __future__ import annotations

import pytest

from src.pipelines.bronze.load_dim import (
    DIM_TABLES,
    SOURCE_FILES,
    source_path_for,
    target_table_for,
)


@pytest.mark.parametrize("table", DIM_TABLES)
def test_every_dim_has_a_source_file(table):
    """If a new dim is added, the missing entry in SOURCE_FILES is loud."""
    assert table in SOURCE_FILES
    assert SOURCE_FILES[table].endswith(".csv")


def test_source_path_uses_volume_layout():
    assert source_path_for("c", "customer") == "/Volumes/c/source/raw/olist_customers_dataset.csv"


def test_target_table_lands_in_bronze_schema():
    assert target_table_for("c", "customer") == "c.bronze.customer"
