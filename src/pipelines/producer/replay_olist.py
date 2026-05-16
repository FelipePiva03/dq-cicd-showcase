"""
Producer: Replay the 4 Olist event tables as a stream
======================================================

The Olist dataset (Kaggle) is a static historical snapshot. To exercise
Structured Streaming + Auto Loader honestly, we re-emit the 4 *event* tables
in time-windowed micro-batches keyed off each row's effective timestamp:

  orders            -> order_purchase_timestamp
  order_items       -> parent order's order_purchase_timestamp (via join)
  order_payments    -> parent order's order_purchase_timestamp (via join)
  order_reviews     -> review_creation_date  (late-arriving — chega dias depois)

That last one is the whole point of using streaming here: reviews arrive
*after* their parent order, so silver MERGE has to handle out-of-order
late events idempotently.

Dimensions are NOT handled by this script — they're batch-loaded by the
`batch_dimensions` job. See docs/ARCHITECTURE.md.
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession, functions as F


EVENT_TABLES = ("orders", "order_items", "order_payments", "order_reviews")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument(
        "--source-path",
        default=None,
        help="Override raw CSV root. Default: /Volumes/{catalog}/source/raw",
    )
    parser.add_argument(
        "--landing-path",
        default=None,
        help="Override landing root. Default: /Volumes/{catalog}/landing/events",
    )
    parser.add_argument("--window-days", type=int, default=7)
    parser.add_argument(
        "--tables",
        default=",".join(EVENT_TABLES),
        help=f"Comma-separated subset of {EVENT_TABLES}",
    )
    return parser.parse_args()


def attach_effective_ts(
    table: str, df: DataFrame, orders_df: DataFrame
) -> DataFrame:
    """Adds `_effective_ts` (date) used to bucket rows into replay windows.

    orders/items/payments share the order's purchase timestamp.
    reviews use review_creation_date (naturally later — the late-arriving case).
    """
    if table == "orders":
        return df.withColumn(
            "_effective_ts", F.to_date("order_purchase_timestamp")
        )
    if table in ("order_items", "order_payments"):
        keyed = orders_df.select(
            "order_id",
            F.to_date("order_purchase_timestamp").alias("_effective_ts"),
        )
        return df.join(F.broadcast(keyed), on="order_id", how="inner")
    if table == "order_reviews":
        return df.withColumn(
            "_effective_ts", F.to_date("review_creation_date")
        )
    raise ValueError(f"Unknown event table: {table}")


def load_raw(spark: SparkSession, source_root: str, table: str) -> DataFrame:
    path = f"{source_root}/olist_{table}_dataset.csv"
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
    )


def write_batch(df: DataFrame, target_dir: str) -> int:
    """Writes one CSV file per batch directory. Returns row count."""
    count = df.count()
    if count == 0:
        return 0
    (
        df.drop("_effective_ts")
        .coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(target_dir)
    )
    return count


def replay(
    spark: SparkSession,
    source_root: str,
    landing_root: str,
    tables: list[str],
    window_days: int,
) -> dict[str, int]:
    """Replays every requested table into landing in time-windowed batches.

    Returns a {table: total_rows_written} summary so callers (and tests)
    can assert on it.
    """
    orders_df = load_raw(spark, source_root, "orders")
    keyed = {
        t: attach_effective_ts(t, load_raw(spark, source_root, t), orders_df)
        for t in tables
    }

    per_table_bounds = [
        k.agg(F.min("_effective_ts"), F.max("_effective_ts")).first()
        for k in keyed.values()
    ]
    mins = [b[0] for b in per_table_bounds if b[0] is not None]
    maxs = [b[1] for b in per_table_bounds if b[1] is not None]
    if not mins:
        print("[producer] no rows in any table — nothing to replay")
        return {t: 0 for t in tables}
    global_min, global_max = min(mins), max(maxs)
    print(f"[producer] global date range: {global_min} -> {global_max}")

    totals: dict[str, int] = {t: 0 for t in tables}
    cursor: date = global_min
    batch_id = 0

    while cursor <= global_max:
        end = cursor + timedelta(days=window_days)
        for table, df in keyed.items():
            batch_df = df.filter(
                (F.col("_effective_ts") >= F.lit(cursor))
                & (F.col("_effective_ts") < F.lit(end))
            )
            target = f"{landing_root}/{table}/batch_{batch_id:04d}"
            n = write_batch(batch_df, target)
            if n:
                print(f"[producer] {table:14s} batch {batch_id:04d}  rows={n}")
                totals[table] += n
        cursor = end
        batch_id += 1

    print(f"[producer] done — totals: {totals}")
    return totals


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    source_root = args.source_path or f"/Volumes/{args.catalog}/source/raw"
    landing_root = args.landing_path or f"/Volumes/{args.catalog}/landing/events"
    tables = [t.strip() for t in args.tables.split(",") if t.strip()]
    for t in tables:
        if t not in EVENT_TABLES:
            raise SystemExit(f"Unknown table {t!r}. Allowed: {EVENT_TABLES}")

    replay(spark, source_root, landing_root, tables, args.window_days)


if __name__ == "__main__":
    main()
