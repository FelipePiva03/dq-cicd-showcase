"""
Bronze: Auto Loader ingestion of Olist event tables
====================================================

One script, one task per table — driven by `--table`. Auto Loader watches
`/Volumes/{catalog}/landing/events/{table}/` (populated by the producer)
and appends every new file into `bronze.{table}`, preserving the data
exactly as it arrived plus two observability columns.

Design choices:
  - `cloudFiles.schemaLocation` (per table) is mandatory — schema-evolution
    metadata lives there. Lose it and you re-ingest everything.
  - `mergeSchema=true` lets new columns flow through automatically;
    type-breaking changes still fail loudly, which is what we want.
  - `multiLine=true` + escape quoting handles review comments containing
    commas, quotes, or newlines without corrupting rows.
  - `availableNow=True` makes the job batch-like for cost control on
    Free Trial; swap to `trigger(processingTime=...)` for true streaming.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F


EVENT_TABLES = ("orders", "order_items", "order_payments", "order_reviews")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--table", required=True, choices=EVENT_TABLES)
    return parser.parse_args()


def paths_for(catalog: str, table: str) -> dict[str, str]:
    """Returns every storage path bronze ingestion needs for one table."""
    return {
        "landing": f"/Volumes/{catalog}/landing/events/{table}",
        "schema_location": f"/Volumes/{catalog}/_schemas/bronze_{table}",
        "checkpoint": f"/Volumes/{catalog}/_checkpoints/bronze_{table}",
        "target_table": f"{catalog}.bronze.{table}",
    }


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    p = paths_for(args.catalog, args.table)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.bronze")

    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", p["schema_location"])
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .option("mergeSchema", "true")
        .load(p["landing"])
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

    (
        df.writeStream.format("delta")
        .option("checkpointLocation", p["checkpoint"])
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(p["target_table"])
    )

    print(f"[bronze] {args.table:14s} streamed into {p['target_table']}")


if __name__ == "__main__":
    main()
