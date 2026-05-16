"""Unit tests for bronze.ingest_events path conventions."""

from __future__ import annotations

import pytest

from src.pipelines.bronze.ingest_events import EVENT_TABLES, paths_for


def test_paths_for_orders_catalog_dq_showcase_dev():
    p = paths_for("dq_showcase_dev", "orders")
    assert p == {
        "landing": "/Volumes/dq_showcase_dev/landing/events/orders",
        "schema_location": "/Volumes/dq_showcase_dev/_meta/cloudfiles/bronze_orders",
        "checkpoint": "/Volumes/dq_showcase_dev/_meta/checkpoints/bronze_orders",
        "target_table": "dq_showcase_dev.bronze.orders",
    }


@pytest.mark.parametrize("table", EVENT_TABLES)
def test_paths_target_table_is_in_bronze_schema(table):
    """Every event table must land in the bronze schema, not anywhere else."""
    p = paths_for("any_catalog", table)
    assert p["target_table"] == f"any_catalog.bronze.{table}"


@pytest.mark.parametrize("table", EVENT_TABLES)
def test_paths_schema_and_checkpoint_are_per_table(table):
    """Schema location and checkpoint MUST be per-table — sharing them across
    tables silently breaks Auto Loader's schema inference and checkpointing."""
    p = paths_for("c", table)
    assert table in p["schema_location"]
    assert table in p["checkpoint"]
    assert p["schema_location"] != p["checkpoint"]
