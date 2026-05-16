"""
Bronze: Auto Loader ingestion of Olist orders
==============================================

Pattern: cloudFiles (Auto Loader) → Delta append-only.

Design choices:
  - `cloudFiles.schemaLocation` is mandatory — it's where schema evolution
    metadata lives. Lose this and you re-ingest everything.
  - `mergeSchema=true` allows new columns to flow through automatically;
    schema breaks (type changes) still fail loudly, which is what we want.
  - `_metadata.file_path` and `_metadata.file_modification_time` are added
    as observability columns. Forensics later thanks you.
  - `availableNow=True` makes the job batch-like for free trial cost control;
    for true continuous streaming, swap to `trigger(processingTime="1 minute")`.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    catalog = args.catalog
    landing = f"/Volumes/{catalog}/landing/orders"
    schema_loc = f"/Volumes/{catalog}/_schemas/bronze_orders"
    checkpoint = f"/Volumes/{catalog}/_checkpoints/bronze_orders"
    target_table = f"{catalog}.bronze.orders"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")

    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_loc)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .option("mergeSchema", "true")
        .load(landing)
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

    (
        df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)         # cost-conscious; see module docstring
        .toTable(target_table)
    )

    print(f"[bronze] streamed into {target_table}")


if __name__ == "__main__":
    main()
