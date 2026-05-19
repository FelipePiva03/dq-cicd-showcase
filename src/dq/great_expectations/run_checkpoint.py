"""
Great Expectations gate — generalized over any contracted table (GX 1.x)
=========================================================================

One script, one task per contract — driven by `--layer` + `--table`. Loads
the contract from `contracts/{layer}/{table}.yml`, builds a GX
ExpectationSuite via the to_gx_suite adapter, runs every expectation
against the live Delta table at `{catalog}.{layer}.{table}`, and decides
exit code based on the *severity* of any failures:

  - critical failure  → exit 1 (blocks downstream)
  - warning  failure  → exit 0 (log + persist results, downstream continues)
  - all pass          → exit 0

Severity lives in each expectation's `meta.severity` (set by to_gx_suite
from the contract's `severity` field). Anything not tagged defaults to
"warning" — explicit critical tagging is the policy.

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

Why GX everywhere (not Soda for dims/gold):
  - Soda Core 3.3.7 raises `notebook command received after detach` on
    serverless mid-scan; Soda 3.5.6 raises `Cannot start a remote Spark
    session because there is a regular Spark session already running`.
  - GX 1.x works cleanly. The contract layer abstracts over the runner
    so this swap was a one-script rewrite.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

# Make `src/` importable AND resolve `contracts/` dir. Databricks
# spark_python_task does NOT set __file__ (runs via exec()).
_script = Path(sys.argv[0]).resolve()
_CONTRACTS_DIR = Path("contracts")
for _p in (_script, *_script.parents):
    if _p.name == "src":
        sys.path.insert(0, str(_p))
        if (_p.parent / "contracts").is_dir():
            _CONTRACTS_DIR = _p.parent / "contracts"
        break
    if (_p / "src").is_dir():
        sys.path.insert(0, str(_p / "src"))
        if (_p / "contracts").is_dir():
            _CONTRACTS_DIR = _p / "contracts"
        break

import great_expectations as gx  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

from dq.contracts import load_contract, to_gx_suite  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument(
        "--layer",
        required=True,
        choices=("silver", "gold"),
        help="Contract layer — `silver` or `gold`. Combined with --table to "
        "locate contracts/{layer}/{table}.yml and the Delta table "
        "{catalog}.{layer}.{table}.",
    )
    parser.add_argument(
        "--table",
        required=True,
        help="Contract table name (e.g. fact_orders, dim_customer, daily_orders).",
    )
    parser.add_argument(
        "--contracts-dir",
        default=str(_CONTRACTS_DIR),
        help="Root directory of `contracts/{layer}/{table}.yml` files. "
        "Default is auto-resolved at boot.",
    )
    return parser.parse_args()


def classify_failures(result) -> tuple[list[dict], list[dict]]:
    """Splits the result.results list into (critical_failed, warning_failed)."""
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
    asset: str,
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
                asset,
                bool(result_payload.get("success")),
                n_critical,
                n_warning,
                json.dumps(result_payload, default=str),
            )
        ],
        "run_ts timestamp, tool string, asset string, overall_success boolean, "
        "n_critical_failed int, n_warning_failed int, payload string",
    )
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        f"{catalog}.dq.run_results"
    )


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    full_table = f"{args.catalog}.{args.layer}.{args.table}"
    asset = f"{args.layer}.{args.table}"
    print(f"[gx] validating {full_table}")
    df = spark.read.table(full_table)

    contract = load_contract(args.layer, args.table, args.contracts_dir)
    suite = to_gx_suite(contract)

    context = gx.get_context(mode="ephemeral")
    suite = context.suites.add(suite)

    # persist=False required on Databricks serverless (rejects PERSIST TABLE).
    data_source = context.data_sources.add_or_update_spark(name="spark_runtime", persist=False)
    data_asset = data_source.add_dataframe_asset(name=asset.replace(".", "_"))
    batch_definition = data_asset.add_batch_definition_whole_dataframe(name="whole")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    result = batch.validate(suite)

    critical, warning = classify_failures(result)
    persist_results(
        spark,
        args.catalog,
        asset,
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
        print(f"[gx] {asset} FAILED (critical)")
        sys.exit(1)

    print(f"[gx] {asset} passed ({len(warning)} warnings)")


if __name__ == "__main__":
    main()
