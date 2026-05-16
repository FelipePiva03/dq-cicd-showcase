"""Shared pytest fixtures — local Spark session for integration tests."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """A local Spark session configured for Delta Lake."""
    return (
        SparkSession.builder.master("local[2]")
        .appName("dq-cicd-showcase-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
