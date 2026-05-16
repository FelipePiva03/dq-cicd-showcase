"""
Producer: Replay Olist CSVs as a stream
========================================

The Olist dataset (Kaggle) is a static historical snapshot. To exercise
Structured Streaming + Auto Loader honestly, we partition the orders by
`order_purchase_timestamp` and write small CSV files into a landing volume
on a configurable cadence. Auto Loader (Bronze) then picks them up
incrementally — exactly like real-world late-arriving data.

Why this approach (vs. faking with `rate` source):
  - Exercises Auto Loader's schema inference and evolution
  - Keeps the pipeline portable: same code works on real arriving files
  - Produces realistic DQ failures (nulls, duplicates, late events)
"""

from __future__ import annotations

import argparse
from datetime import timedelta

from pyspark.sql import SparkSession, functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--batch-size-days", type=int, default=7)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    source_path = f"/Volumes/{args.catalog}/source/raw/olist_orders_dataset.csv"
    landing_path = f"/Volumes/{args.catalog}/landing/orders"

    print(f"[producer] reading from {source_path}")
    orders = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(source_path)
        .withColumn("order_purchase_date", F.to_date("order_purchase_timestamp"))
    )

    # Group purchase dates into batches of N days each
    min_date, max_date = orders.agg(
        F.min("order_purchase_date"), F.max("order_purchase_date")
    ).first()

    print(f"[producer] date range: {min_date} → {max_date}")
    cursor = min_date
    batch_id = 0

    while cursor <= max_date:
        end = cursor + timedelta(days=args.batch_size_days)
        batch = orders.filter(
            (F.col("order_purchase_date") >= F.lit(cursor))
            & (F.col("order_purchase_date") < F.lit(end))
        )

        if batch.count() > 0:
            out = f"{landing_path}/batch_{batch_id:04d}"
            batch.drop("order_purchase_date").coalesce(1).write.mode("overwrite") \
                .option("header", "true").csv(out)
            print(f"[producer] wrote batch {batch_id} ({cursor} → {end}) to {out}")
            batch_id += 1

        cursor = end

    print(f"[producer] done — produced {batch_id} batches")


if __name__ == "__main__":
    main()
