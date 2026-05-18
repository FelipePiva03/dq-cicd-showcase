"""
Silver: Streaming MERGE into conformed fact tables (+ quarantine)
==================================================================

One script, one task per fact — driven by `--table`. Each microbatch:
  1. dedupe by natural key (latest `_ingestion_ts` wins)
  2. conform (type cast, normalize, derive columns)
  3. split on HARD_RULES:
       - valid rows MERGE into  silver.fact_{table}
       - invalid rows APPEND to quarantine.fact_{table}
         with `_quarantine_reason` and `_quarantine_ts`

Why split *after* conform (not before):
  - try_cast nulls invalid numerics → hard rule "price IS NULL" catches
    them. Pre-conform we'd see strings only and couldn't tell good from
    bad without re-parsing.

Why two writes per batch:
  - Bad rows never reach silver — silver stays a trusted source for
    GX gate + gold marts.
  - Quarantine is preserved for forensics ("which rows did we drop and why?").
  - Append-only on quarantine means no MERGE complexity (we never need to
    'update' a quarantined row; it's a tombstone).

Hard rule vs soft rule:
  - HARD_RULES (this module): structural integrity violations — null PK,
    null FK to required dim, null after try_cast. If these passed, silver
    would be broken.
  - GX gate (run_checkpoint.py): business semantic violations —
    `days_to_delivery >= 0`, `review_score BETWEEN 1 AND 5`. If these
    fail, silver is still well-formed but the data is suspect.
"""

from __future__ import annotations

import argparse

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

EVENT_TABLES = ("orders", "order_items", "order_payments", "order_reviews")

NATURAL_KEYS: dict[str, list[str]] = {
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "order_payments": ["order_id", "payment_sequential"],
    "order_reviews": ["review_id"],
}

# CASE expression per table. Returns the rule name if violated, NULL if clean.
# Order matters: first matching WHEN wins, so list most-critical rules first.
HARD_RULES: dict[str, str] = {
    "orders": """
        CASE
            WHEN order_id IS NULL                 THEN 'order_id_null'
            WHEN customer_id IS NULL              THEN 'customer_id_null'
            WHEN order_status IS NULL             THEN 'order_status_null'
            WHEN order_purchase_timestamp IS NULL THEN 'purchase_ts_unparseable'
            ELSE NULL
        END
    """,
    "order_items": """
        CASE
            WHEN order_id IS NULL      THEN 'order_id_null'
            WHEN order_item_id IS NULL THEN 'order_item_id_null'
            WHEN product_id IS NULL    THEN 'product_id_null'
            WHEN price IS NULL         THEN 'price_unparseable'
            ELSE NULL
        END
    """,
    "order_payments": """
        CASE
            WHEN order_id IS NULL           THEN 'order_id_null'
            WHEN payment_sequential IS NULL THEN 'payment_sequential_null'
            WHEN payment_value IS NULL      THEN 'payment_value_unparseable'
            ELSE NULL
        END
    """,
    "order_reviews": """
        CASE
            WHEN review_id IS NULL    THEN 'review_id_null'
            WHEN order_id IS NULL     THEN 'order_id_null'
            WHEN review_score IS NULL THEN 'review_score_unparseable'
            ELSE NULL
        END
    """,
}


# ---------------------------------------------------------------------------
# Per-table conform functions
# ---------------------------------------------------------------------------
def conform_orders(df: DataFrame) -> DataFrame:
    # try_to_timestamp (not to_timestamp): under ANSI mode (default on DBR 15+)
    # a single malformed timestamp string fails the whole microbatch. try_*
    # variants return null instead — and the hard rule catches the null.
    return (
        df.withColumn("order_purchase_timestamp", F.try_to_timestamp("order_purchase_timestamp"))
        .withColumn("order_approved_at", F.try_to_timestamp("order_approved_at"))
        .withColumn(
            "order_delivered_carrier_date", F.try_to_timestamp("order_delivered_carrier_date")
        )
        .withColumn(
            "order_delivered_customer_date", F.try_to_timestamp("order_delivered_customer_date")
        )
        .withColumn(
            "order_estimated_delivery_date", F.try_to_timestamp("order_estimated_delivery_date")
        )
        .withColumn("order_status", F.lower(F.col("order_status")))
        .withColumn(
            "days_to_delivery",
            F.datediff(
                F.col("order_delivered_customer_date"),
                F.col("order_purchase_timestamp"),
            ),
        )
        .select(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "days_to_delivery",
            F.current_timestamp().alias("_silver_processed_ts"),
        )
    )


def conform_order_items(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("price", F.expr("try_cast(price AS decimal(10,2))"))
        .withColumn("freight_value", F.expr("try_cast(freight_value AS decimal(10,2))"))
        .withColumn("shipping_limit_date", F.try_to_timestamp("shipping_limit_date"))
        .select(
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
            F.current_timestamp().alias("_silver_processed_ts"),
        )
    )


def conform_order_payments(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("payment_type", F.lower(F.col("payment_type")))
        .withColumn("payment_value", F.expr("try_cast(payment_value AS decimal(10,2))"))
        .select(
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
            F.current_timestamp().alias("_silver_processed_ts"),
        )
    )


def conform_order_reviews(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("review_creation_date", F.try_to_timestamp("review_creation_date"))
        .withColumn("review_answer_timestamp", F.try_to_timestamp("review_answer_timestamp"))
        .withColumn("review_score", F.expr("try_cast(review_score AS int)"))
        .select(
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp",
            F.current_timestamp().alias("_silver_processed_ts"),
        )
    )


CONFORMERS = {
    "orders": conform_orders,
    "order_items": conform_order_items,
    "order_payments": conform_order_payments,
    "order_reviews": conform_order_reviews,
}


# ---------------------------------------------------------------------------
# Shared transforms: dedupe + quarantine split
# ---------------------------------------------------------------------------
def dedupe_by_key(df: DataFrame, keys: list[str], order_col: str = "_ingestion_ts") -> DataFrame:
    """Keeps the latest row per natural key (ordered by `order_col` desc).

    Required before MERGE: without it, a microbatch with multiple rows for
    the same key raises MULTI_SOURCE_ROW_MATCH_FOR_TARGET.
    """
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def split_quarantine(df: DataFrame, table: str) -> tuple[DataFrame, DataFrame]:
    """Splits a conformed DF into (valid_rows, quarantined_rows).

    Quarantined rows keep the silver schema plus two metadata columns:
    `_quarantine_reason` (which HARD_RULES rule fired) and
    `_quarantine_ts` (wall-clock when the row was rejected).
    """
    tagged = df.withColumn("_quarantine_reason", F.expr(HARD_RULES[table]))
    valid = tagged.filter("_quarantine_reason IS NULL").drop("_quarantine_reason")
    quarantined = tagged.filter("_quarantine_reason IS NOT NULL").withColumn(
        "_quarantine_ts", F.current_timestamp()
    )
    return valid, quarantined


# ---------------------------------------------------------------------------
# foreachBatch sink
# ---------------------------------------------------------------------------
def upsert_to_silver(
    spark: SparkSession,
    silver_table: str,
    quarantine_table: str,
    table_name: str,
):
    """foreachBatch callback: dedupe → conform → split → MERGE valid + APPEND quarantine."""
    keys = NATURAL_KEYS[table_name]
    conform_fn = CONFORMERS[table_name]
    merge_condition = " AND ".join(f"t.{k} = s.{k}" for k in keys)

    def _upsert(batch_df: DataFrame, batch_id: int) -> None:
        deduped = dedupe_by_key(batch_df, keys)
        conformed = conform_fn(deduped)
        valid, quarantined = split_quarantine(conformed, table_name)

        # Quarantine first — even if MERGE fails, we keep the evidence.
        # head(1) is a cheap non-empty probe (one task, early termination).
        if quarantined.head(1):
            mode = "append" if spark.catalog.tableExists(quarantine_table) else "overwrite"
            quarantined.write.format("delta").mode(mode).saveAsTable(quarantine_table)
            print(f"[quarantine] {table_name:14s} batch {batch_id}: rows appended")

        if not valid.head(1):
            print(f"[silver] {table_name:14s} batch {batch_id}: no valid rows, skipping MERGE")
            return

        if not spark.catalog.tableExists(silver_table):
            valid.write.format("delta").saveAsTable(silver_table)
            print(f"[silver] {table_name:14s} batch {batch_id}: created table")
            return

        delta_tbl = DeltaTable.forName(spark, silver_table)
        (
            delta_tbl.alias("t")
            .merge(valid.alias("s"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"[silver] {table_name:14s} batch {batch_id}: MERGE complete")

    return _upsert


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--table", required=True, choices=EVENT_TABLES)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    source_table = f"{args.catalog}.bronze.{args.table}"
    silver_table = f"{args.catalog}.silver.fact_{args.table}"
    quarantine_table = f"{args.catalog}.quarantine.fact_{args.table}"
    checkpoint = f"/Volumes/{args.catalog}/_meta/checkpoints/silver_fact_{args.table}"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.silver")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.quarantine")

    (
        spark.readStream.table(source_table)
        .writeStream.foreachBatch(
            upsert_to_silver(spark, silver_table, quarantine_table, args.table)
        )
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )

    print(f"[silver] {args.table:14s} done — silver={silver_table}, quarantine={quarantine_table}")


if __name__ == "__main__":
    main()
