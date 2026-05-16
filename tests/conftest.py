"""Shared pytest fixtures — local Spark session for integration tests."""

from __future__ import annotations

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """A local Spark session with Delta Lake JARs auto-resolved by Maven.

    `configure_spark_with_delta_pip` is the official delta-spark helper —
    it tells Spark which Maven coords to download on first run. Without it,
    you get `ClassNotFoundException: org.apache.spark.sql.delta.catalog.DeltaCatalog`.
    """
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("dq-cicd-showcase-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
