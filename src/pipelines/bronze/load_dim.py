"""
Bronze: Batch load of Olist dimension tables
=============================================

Dimensions are master data, not events — full-refresh on each run is the
honest pattern. We use a plain `spark.read.csv` + `mode("overwrite")`:
  - The whole table is small (≤1M rows for geolocation, others are tiny)
  - The CSVs are static snapshots
  - There's no incremental signal to chase
  - COPY INTO would add tracking complexity for zero benefit here

Output preserves bronze conventions: data as-is plus two observability
columns (`_ingestion_ts`, `_source_file`).
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DIM_TABLES = ("customer", "product", "seller", "geolocation", "category")

# Maps the bronze table name to the raw CSV filename. Olist names them
# inconsistently — events follow `olist_<table>_dataset.csv`, but the
# category translation is its own pattern.
SOURCE_FILES = {
    "customer": "olist_customers_dataset.csv",
    "product": "olist_products_dataset.csv",
    "seller": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category": "product_category_name_translation.csv",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--table", required=True, choices=DIM_TABLES)
    return parser.parse_args()


def source_path_for(catalog: str, table: str) -> str:
    return f"/Volumes/{catalog}/source/raw/{SOURCE_FILES[table]}"


def target_table_for(catalog: str, table: str) -> str:
    return f"{catalog}.bronze.{table}"


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    source = source_path_for(args.catalog, args.table)
    target = target_table_for(args.catalog, args.table)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.bronze")

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        # multiLine + escape protect against quoted free-text columns
        # (less of an issue for dims, but cheap insurance).
        .option("multiLine", "true")
        .option("escape", '"')
        .load(source)
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_source_file", F.lit(source))
    )

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )

    print(f"[bronze-dim] {args.table:12s} loaded into {target}")


if __name__ == "__main__":
    main()
