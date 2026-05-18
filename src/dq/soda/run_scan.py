"""
Soda Core scan runner — parametrized, generic
==============================================

Executes a Soda check file against any set of Delta tables via the
soda-spark-df adapter. The caller passes:

  --tables    comma-separated `schema.table` list to register as temp views.
              The view name is the last path component (e.g. `silver.dim_customer`
              becomes the temp view `dim_customer` which the checks file
              references as `checks for dim_customer:`).
  --checks-file  YAML file inside --checks-dir (default src/dq/soda/checks).

Soda's strengths visible here:
  - One YAML file declares many checks across many tables; the runner stays tiny.
  - `warn: when ...` separates "informational" from "fail" — naturally tiered.
  - Cross-table failed-rows queries via `fail query: SELECT ... FROM ... JOIN ...`.

Contrast with GX (see ../great_expectations/run_checkpoint.py): more code,
more concepts (Datasources, Checkpoints, Suites, Expectations), but richer
result objects per expectation.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from soda.scan import Scan

DEFAULT_CHECKS_DIR = Path("src/dq/soda/checks")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--checks-file", required=True)
    parser.add_argument(
        "--checks-dir",
        default=str(DEFAULT_CHECKS_DIR),
        help="Directory containing the checks YAML. Bundle-root-relative; on "
        "Databricks pass the absolute path via ${workspace.file_path}/...",
    )
    parser.add_argument(
        "--tables",
        required=True,
        help="Comma-separated `schema.table` list (catalog implicit). Each "
        "is registered as a temp view named after the last path component.",
    )
    return parser.parse_args()


def register_views(spark: SparkSession, catalog: str, table_list: list[str]) -> list[str]:
    """Registers each `schema.table` from the catalog as a temp view.

    View name = last path component (so `silver.dim_customer` → `dim_customer`).
    Returns the list of view names so the caller can log what was registered.
    """
    view_names = []
    for st in table_list:
        full = f"{catalog}.{st}"
        view = st.split(".")[-1]
        spark.read.table(full).createOrReplaceTempView(view)
        view_names.append(view)
    return view_names


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    tables = [t.strip() for t in args.tables.split(",") if t.strip()]
    views = register_views(spark, args.catalog, tables)
    print(f"[soda] registered temp views: {views}")

    checks_path = Path(args.checks_dir) / args.checks_file

    scan = Scan()
    scan.set_scan_definition_name(f"{args.checks_file}_{datetime.utcnow().isoformat()}")
    scan.set_data_source_name("spark_df")
    scan.add_spark_session(spark, data_source_name="spark_df")
    scan.add_sodacl_yaml_file(str(checks_path))
    scan.execute()

    print(scan.get_logs_text())
    _persist_results(spark, args.catalog, args.checks_file, scan.get_scan_results())

    if scan.has_check_fails():
        print(f"[soda] {args.checks_file} FAILED (one or more `fail` checks broke)")
        sys.exit(1)
    if scan.has_check_warns():
        print(f"[soda] {args.checks_file} passed with warnings")
    else:
        print(f"[soda] {args.checks_file} passed cleanly")


def _persist_results(spark: SparkSession, catalog: str, checks_file: str, payload: dict) -> None:
    """Appends Soda scan results to the shared DQ telemetry table."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.dq")
    df = spark.createDataFrame(
        [(datetime.utcnow(), "soda_core", checks_file, str(payload))],
        "run_ts timestamp, tool string, checks_file string, payload string",
    )
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        f"{catalog}.dq.run_results"
    )


if __name__ == "__main__":
    main()
