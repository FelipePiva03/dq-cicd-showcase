"""
Great Expectations gate for silver fact tables — tiered severity (GX 1.x)
==========================================================================

One script, one task per fact — driven by `--table`. Loads the suite from
JSON (using GX 1.x native class names), runs every expectation against the
live silver Delta table, and decides exit code based on the *severity* of
any failures:

  - critical failure  → exit 1 (blocks gold downstream)
  - warning  failure  → exit 0 (log + persist results, gold continues)
  - all pass          → exit 0

Severity lives in each expectation's `meta.severity`. Anything not tagged
defaults to "warning" — explicit critical tagging is the policy.

Full validation results persist to `{catalog}.dq.run_results` regardless
of exit code, so the DQ dashboard can show trends and warning-level drift.

Why GX 1.x (not 0.18):
  - GX 0.18 hardcodes `.persist()` on the batch DataFrame in
    `SparkDFExecutionEngine`. Databricks serverless rejects PERSIST
    (NOT_SUPPORTED_WITH_SERVERLESS). Setting persist=False on the
    datasource workaround didn't help — most aggregation-style
    expectations still silent-fail (success=False, result={}) because
    GX 0.18 was never tested against Spark Connect.
  - GX 1.x has explicit serverless support and Spark Connect coverage.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

# Make `src/` importable for Databricks spark_python_task.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import great_expectations as gx
from pyspark.sql import SparkSession

from dq.contracts import load_contract, to_gx_suite

FACT_TABLES = ("orders", "order_items", "order_payments", "order_reviews")
DEFAULT_CONTRACTS_DIR = Path("contracts")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--table", required=True, choices=FACT_TABLES)
    parser.add_argument(
        "--contracts-dir",
        default=str(DEFAULT_CONTRACTS_DIR),
        help="Root directory of `contracts/{layer}/{table}.yml` files. "
        "Default is bundle-root-relative; on Databricks pass the absolute "
        "path via ${workspace.file_path}/contracts.",
    )
    return parser.parse_args()


def classify_failures(result) -> tuple[list[dict], list[dict]]:
    """Splits the result.results list into (critical_failed, warning_failed).

    Each item captures the GX result dict so we can show *why* it failed
    (observed_value / unexpected_count / element_count / exception_info
    depending on the expectation type).
    """
    critical, warning = [], []
    for r in result.results:
        if r.success:
            continue
        cfg = r.expectation_config
        meta = cfg.meta or {}
        severity = meta.get("severity", "warning")
        gx_result = r.result or {}
        diagnostic = (
            gx_result.get("observed_value")
            or gx_result.get("unexpected_count")
            or gx_result.get("element_count")
            or gx_result.get("exception_info")
            or gx_result
        )
        kwargs = cfg.kwargs or {}
        record = {
            "expectation_type": cfg.type,
            "column": kwargs.get("column") or kwargs.get("column_list") or "(table-level)",
            "severity": severity,
            "diagnostic": diagnostic,
        }
        (critical if severity == "critical" else warning).append(record)
    return critical, warning


def persist_results(
    spark: SparkSession,
    catalog: str,
    table: str,
    result_payload: dict,
    n_critical: int,
    n_warning: int,
) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.dq")
    df = spark.createDataFrame(
        [
            (
                datetime.utcnow(),
                "great_expectations",
                f"silver_fact_{table}",
                bool(result_payload.get("success")),
                n_critical,
                n_warning,
                json.dumps(result_payload, default=str),
            )
        ],
        "run_ts timestamp, tool string, asset string, overall_success boolean, "
        "n_critical_failed int, n_warning_failed int, payload string",
    )
    df.write.format("delta").mode("append").saveAsTable(f"{catalog}.dq.run_results")


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    silver_table = f"{args.catalog}.silver.fact_{args.table}"
    print(f"[gx] validating {silver_table}")
    silver_df = spark.read.table(silver_table)

    contract = load_contract("silver", f"fact_{args.table}", args.contracts_dir)
    suite = to_gx_suite(contract)

    # GX 1.x fluent API — no YAML, no checkpoint config, no ephemeral persist.
    context = gx.get_context(mode="ephemeral")
    suite = context.suites.add(suite)

    # persist=False is required on Databricks serverless: GX (both 0.18 and
    # 1.x) defaults to calling .persist() on the batch DataFrame to cache it
    # across expectations, but serverless rejects PERSIST TABLE with
    # NOT_SUPPORTED_WITH_SERVERLESS (SQLSTATE 0A000). Small perf cost per
    # extra evaluation, fine for our suite sizes.
    data_source = context.data_sources.add_or_update_spark(name="spark_runtime", persist=False)
    data_asset = data_source.add_dataframe_asset(name=f"silver_fact_{args.table}")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(name="whole")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": silver_df})

    result = batch.validate(suite)

    critical, warning = classify_failures(result)
    persist_results(
        spark,
        args.catalog,
        args.table,
        result.to_json_dict(),
        len(critical),
        len(warning),
    )

    if warning:
        print(f"[gx] {len(warning)} WARNING expectation(s) failed:")
        for w in warning:
            print(f"     - {w['expectation_type']} on {w['column']}  diag={w['diagnostic']}")
    if critical:
        print(f"[gx] {len(critical)} CRITICAL expectation(s) failed:")
        for c in critical:
            print(f"     - {c['expectation_type']} on {c['column']}  diag={c['diagnostic']}")
        print(f"[gx] silver_fact_{args.table} FAILED (critical)")
        sys.exit(1)

    print(f"[gx] silver_fact_{args.table} passed ({len(warning)} warnings)")


if __name__ == "__main__":
    main()
