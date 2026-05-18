"""
Soda Core scan runner — driven by data contracts
=================================================

Executes Soda Core against a set of Delta tables, generating the SodaCL
YAML on the fly from `contracts/{schema}/{table}.yml`.

Arguments:
  --catalog        Unity Catalog name (passed via DAB variable)
  --tables         Comma-separated `schema.table` list. Each table is
                   registered as a temp view named after the table part
                   (so `silver.dim_customer` → view `dim_customer`), and
                   its contract is loaded from `contracts/{schema}/{table}.yml`.
  --contracts-dir  Root containing `contracts/{layer}/`. Bundle-root-relative;
                   on Databricks pass `${workspace.file_path}/contracts`.
  --scan-name      Logical scan name persisted to dq.run_results (e.g.
                   `silver_dims`, `gold_marts`).

The actual SodaCL is built in-memory by `dq.contracts.to_soda_yaml` and
fed to Soda via `scan.add_sodacl_yaml_str`. No checks YAML files are
read from disk at runtime — contracts are the single source of truth.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

# Make `src/` importable for Databricks spark_python_task.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import SparkSession
from soda.scan import Scan

from dq.contracts import load_contract, to_soda_yaml

DEFAULT_CONTRACTS_DIR = Path("contracts")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument(
        "--tables",
        required=True,
        help="Comma-separated `schema.table` list (catalog implicit). Each "
        "is registered as a temp view named after the table part.",
    )
    parser.add_argument(
        "--contracts-dir",
        default=str(DEFAULT_CONTRACTS_DIR),
        help="Root directory of `contracts/{layer}/{table}.yml` files.",
    )
    parser.add_argument(
        "--scan-name",
        required=True,
        help="Logical name for this scan (e.g. silver_dims, gold_marts). "
        "Used in scan_definition_name and persisted to dq.run_results.",
    )
    return parser.parse_args()


def register_views(spark: SparkSession, catalog: str, table_list: list[str]) -> list[str]:
    """Register each `schema.table` from `catalog` as a temp view named after the table part."""
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

    contracts = [
        load_contract(
            layer=st.split(".")[0], table=st.split(".")[1], contracts_dir=args.contracts_dir
        )
        for st in tables
    ]
    sodacl = to_soda_yaml(contracts)
    print(
        f"[soda] generated SodaCL ({sum(len(c.expectations) for c in contracts)} checks across {len(contracts)} tables)"
    )

    scan = Scan()
    scan.set_scan_definition_name(f"{args.scan_name}_{datetime.utcnow().isoformat()}")
    scan.set_data_source_name("spark_df")
    scan.add_spark_session(spark, data_source_name="spark_df")
    scan.add_sodacl_yaml_str(sodacl)
    scan.execute()

    print(scan.get_logs_text())
    _persist_results(spark, args.catalog, args.scan_name, scan.get_scan_results())

    if scan.has_check_fails():
        print(f"[soda] {args.scan_name} FAILED (one or more `fail` checks broke)")
        sys.exit(1)
    if scan.has_check_warns():
        print(f"[soda] {args.scan_name} passed with warnings")
    else:
        print(f"[soda] {args.scan_name} passed cleanly")


def _persist_results(spark: SparkSession, catalog: str, scan_name: str, payload: dict) -> None:
    """Appends Soda scan results to the shared DQ telemetry table."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.dq")
    df = spark.createDataFrame(
        [(datetime.utcnow(), "soda_core", scan_name, str(payload))],
        "run_ts timestamp, tool string, scan_name string, payload string",
    )
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        f"{catalog}.dq.run_results"
    )


if __name__ == "__main__":
    main()
