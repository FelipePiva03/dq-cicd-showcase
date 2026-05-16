"""
Great Expectations checkpoint runner
=====================================

Executes a GX checkpoint against a Delta table in the configured catalog and
**fails the job** if any expectation in the suite fails.

Why fail-fast instead of "warn":
  - The whole point of putting GX in the bundle as a task is to be a gate.
  - DQ results are written to a Delta table for dashboarding regardless of
    pass/fail — observability is preserved.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime

import great_expectations as gx
from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--checkpoint", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    # In Databricks, GX context lives in /Workspace; locally, in ./gx
    context = gx.get_context(context_root_dir="src/dq/great_expectations")

    # The asset is registered in the GX YAML config; pulling the live Spark DF here
    silver_df = spark.read.table(f"{args.catalog}.silver.orders")

    batch_request = {
        "datasource_name": "spark_runtime",
        "data_connector_name": "runtime",
        "data_asset_name": "silver_orders",
        "runtime_parameters": {"batch_data": silver_df},
        "batch_identifiers": {"run_id": datetime.utcnow().isoformat()},
    }

    result = context.run_checkpoint(
        checkpoint_name=args.checkpoint,
        batch_request=batch_request,
    )

    # Persist results to a Delta table for the DQ dashboard
    _persist_results(spark, args.catalog, result.to_json_dict())

    if not result["success"]:
        print(f"[gx] ❌ checkpoint {args.checkpoint} FAILED")
        sys.exit(1)
    print(f"[gx] ✅ checkpoint {args.checkpoint} passed")


def _persist_results(spark: SparkSession, catalog: str, payload: dict) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.dq")
    df = spark.createDataFrame(
        [(datetime.utcnow(), "great_expectations", str(payload))],
        "run_ts timestamp, tool string, payload string",
    )
    (
        df.write.format("delta").mode("append").saveAsTable(f"{catalog}.dq.run_results")
    )


if __name__ == "__main__":
    main()
