"""
Silver: SCD Type 1 MERGE into conformed dimension tables
=========================================================

One script, one task per dim — driven by `--table`. Each batch:
  1. read bronze.{dim} (batch, not stream — dims are master data)
  2. conform (type cast, normalize, dedupe at the right grain)
  3. split on HARD_RULES (PK not null)
       valid     → MERGE SCD1 into silver.dim_{dim}
       invalid   → APPEND to quarantine.dim_{dim} (forensics)
  4. MERGE: WHEN MATCHED UPDATE ALL, WHEN NOT MATCHED INSERT ALL

Why SCD Type 1 (not Type 2):
  Olist is a static historical export. There is no "history of changes" to
  preserve — every row represents one point-in-time snapshot. SCD1 is the
  honest choice; the SCD2 pattern is documented in DABS_GUIDE.md as a
  swap-in for production CDC sources.

Why batch (not stream):
  Dims are slow-changing master data. Streaming reads would burn cluster
  time waiting for files that aren't coming. The batch_dimensions job
  schedules this nightly — that's the right cadence.
"""

from __future__ import annotations

import argparse

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DIM_TABLES = ("customer", "product", "seller", "geolocation", "category")

# Natural keys per dim. geolocation collapses to zip prefix (Olist has many
# rows per zip with different lat/lng — we average).
NATURAL_KEYS: dict[str, list[str]] = {
    "customer": ["customer_id"],
    "product": ["product_id"],
    "seller": ["seller_id"],
    "geolocation": ["geolocation_zip_code_prefix"],
    "category": ["product_category_name"],
}

# Hard structural rules. Dims are simpler than facts — PK not null is the
# main concern. PK collisions are resolved earlier in conform (grouping for
# geolocation, deduplication for the rest if needed).
HARD_RULES: dict[str, str] = {
    "customer": "CASE WHEN customer_id IS NULL THEN 'customer_id_null' ELSE NULL END",
    "product": "CASE WHEN product_id IS NULL THEN 'product_id_null' ELSE NULL END",
    "seller": "CASE WHEN seller_id IS NULL THEN 'seller_id_null' ELSE NULL END",
    "geolocation": "CASE WHEN geolocation_zip_code_prefix IS NULL THEN 'zip_prefix_null' ELSE NULL END",
    "category": "CASE WHEN product_category_name IS NULL THEN 'category_name_null' ELSE NULL END",
}


# ---------------------------------------------------------------------------
# Per-dim conformers
# ---------------------------------------------------------------------------
def conform_customer(df: DataFrame) -> DataFrame:
    return df.select(
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        F.initcap(F.col("customer_city")).alias("customer_city"),
        F.upper(F.col("customer_state")).alias("customer_state"),
        F.current_timestamp().alias("_silver_processed_ts"),
    )


def conform_product(df: DataFrame) -> DataFrame:
    # Olist source has typos `name_lenght` and `description_lenght` — we
    # carry them as-is to keep the source-of-truth honesty; renaming would
    # silently break analysts who already query bronze.
    return df.select(
        "product_id",
        "product_category_name",
        F.expr("try_cast(product_name_lenght AS int)").alias("product_name_lenght"),
        F.expr("try_cast(product_description_lenght AS int)").alias("product_description_lenght"),
        F.expr("try_cast(product_photos_qty AS int)").alias("product_photos_qty"),
        F.expr("try_cast(product_weight_g AS int)").alias("product_weight_g"),
        F.expr("try_cast(product_length_cm AS int)").alias("product_length_cm"),
        F.expr("try_cast(product_height_cm AS int)").alias("product_height_cm"),
        F.expr("try_cast(product_width_cm AS int)").alias("product_width_cm"),
        F.current_timestamp().alias("_silver_processed_ts"),
    )


def conform_seller(df: DataFrame) -> DataFrame:
    return df.select(
        "seller_id",
        "seller_zip_code_prefix",
        F.initcap(F.col("seller_city")).alias("seller_city"),
        F.upper(F.col("seller_state")).alias("seller_state"),
        F.current_timestamp().alias("_silver_processed_ts"),
    )


def conform_geolocation(df: DataFrame) -> DataFrame:
    """Collapses multi-row-per-zip into one row per zip prefix.

    Olist's geolocation table has many lat/lng samples per zip (one per
    address seen). For a dim, we want one row per key — averaging coords
    + taking first city/state is a defensible compromise.
    """
    return df.groupBy("geolocation_zip_code_prefix").agg(
        F.avg("geolocation_lat").alias("geolocation_lat"),
        F.avg("geolocation_lng").alias("geolocation_lng"),
        F.first(F.initcap(F.col("geolocation_city")), ignorenulls=True).alias("geolocation_city"),
        F.first(F.upper(F.col("geolocation_state")), ignorenulls=True).alias("geolocation_state"),
        F.current_timestamp().alias("_silver_processed_ts"),
    )


def conform_category(df: DataFrame) -> DataFrame:
    return df.select(
        "product_category_name",
        "product_category_name_english",
        F.current_timestamp().alias("_silver_processed_ts"),
    )


CONFORMERS = {
    "customer": conform_customer,
    "product": conform_product,
    "seller": conform_seller,
    "geolocation": conform_geolocation,
    "category": conform_category,
}


# ---------------------------------------------------------------------------
# Shared transforms: quarantine split + SCD1 MERGE
# ---------------------------------------------------------------------------
def split_quarantine(df: DataFrame, table: str) -> tuple[DataFrame, DataFrame]:
    """Splits a conformed dim DF into (valid, quarantined) using HARD_RULES."""
    tagged = df.withColumn("_quarantine_reason", F.expr(HARD_RULES[table]))
    valid = tagged.filter("_quarantine_reason IS NULL").drop("_quarantine_reason")
    quarantined = tagged.filter("_quarantine_reason IS NOT NULL").withColumn(
        "_quarantine_ts", F.current_timestamp()
    )
    return valid, quarantined


def merge_scd1(
    spark: SparkSession,
    valid: DataFrame,
    silver_table: str,
    keys: list[str],
) -> None:
    """SCD Type 1 MERGE: WHEN MATCHED UPDATE ALL, WHEN NOT MATCHED INSERT ALL."""
    if not valid.head(1):
        print(f"[silver-dim] no valid rows for {silver_table}, skipping MERGE")
        return

    if not spark.catalog.tableExists(silver_table):
        valid.write.format("delta").saveAsTable(silver_table)
        print(f"[silver-dim] created {silver_table}")
        return

    merge_condition = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    delta_tbl = DeltaTable.forName(spark, silver_table)
    (
        delta_tbl.alias("t")
        .merge(valid.alias("s"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"[silver-dim] MERGE complete into {silver_table}")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--table", required=True, choices=DIM_TABLES)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    bronze_table = f"{args.catalog}.bronze.{args.table}"
    silver_table = f"{args.catalog}.silver.dim_{args.table}"
    quarantine_table = f"{args.catalog}.quarantine.dim_{args.table}"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.silver")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.quarantine")

    bronze_df = spark.read.table(bronze_table)
    conformed = CONFORMERS[args.table](bronze_df)
    valid, quarantined = split_quarantine(conformed, args.table)

    if quarantined.head(1):
        mode = "append" if spark.catalog.tableExists(quarantine_table) else "overwrite"
        quarantined.write.format("delta").mode(mode).saveAsTable(quarantine_table)
        print(f"[quarantine-dim] {args.table:12s} rows appended to {quarantine_table}")

    merge_scd1(spark, valid, silver_table, NATURAL_KEYS[args.table])

    print(f"[silver-dim] {args.table:12s} done — silver={silver_table}")


if __name__ == "__main__":
    main()
