"""
Soda Core scan runner
======================

Executes a Soda check file against a Delta table via the soda-spark-df
adapter. Fails the job if any check fails (Soda's `scan.has_check_fails()`).

Soda's strengths visible here:
  - One YAML file declares many checks; the runner stays tiny.
  - `attributes` and `metric` checks support arbitrary SQL expressions.
  - `warn` vs. `fail` thresholds let you tune severity per check.

Contrast with GX (see run_checkpoint.py): more code, more concepts
(Datasources, Checkpoints, Suites, Expectations), but richer Data Docs.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from soda.scan import Scan


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--checks-file", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    # Soda needs DataFrames registered as temp views by name
    silver_orders = spark.read.table(f"{args.catalog}.silver.orders")
    daily_orders = spark.read.table(f"{args.catalog}.gold.daily_orders")
    silver_orders.createOrReplaceTempView("silver_orders")
    daily_orders.createOrReplaceTempView("daily_orders")

    checks_path = Path("src/dq/soda/checks") / args.checks_file

    scan = Scan()
    scan.set_scan_definition_name(f"olist_{datetime.utcnow().isoformat()}")
    scan.set_data_source_name("spark_df")
    scan.add_spark_session(spark, data_source_name="spark_df")
    scan.add_sodacl_yaml_file(str(checks_path))
    scan.execute()

    print(scan.get_logs_text())
    _persist_results(spark, args.catalog, scan.get_scan_results())

    if scan.has_check_fails():
        print(f"[soda] scan {args.checks_file} FAILED")
        sys.exit(1)
    print(f"[soda] scan {args.checks_file} passed")


def _persist_results(spark: SparkSession, catalog: str, payload: dict) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.dq")
    df = spark.createDataFrame(
        [(datetime.utcnow(), "soda_core", str(payload))],
        "run_ts timestamp, tool string, payload string",
    )
    (df.write.format("delta").mode("append").saveAsTable(f"{catalog}.dq.run_results"))


if __name__ == "__main__":
    main()
