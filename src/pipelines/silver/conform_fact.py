"""
Silver: Streaming MERGE into conformed fact tables
===================================================

One script, one task per fact — driven by `--table`. Each table reads from
its `bronze.{table}` source, type-casts + normalizes, dedupes by natural
key (latest `_ingestion_ts` wins), and MERGEs into `silver.fact_{table}`.

Why a single parametrized script:
  - The streaming + MERGE harness is identical across all 4 facts.
  - Per-table differences live in two small dicts (NATURAL_KEYS,
    CONFORMERS), which keeps the divergence visible in one place.

Why dedupe-before-MERGE:
  - When a microbatch carries two rows with the same natural key
    (out-of-order replays, late corrections), MERGE explodes with
    MULTI_SOURCE_ROW_MATCH_FOR_TARGET. Window-based row_number takes
    the latest `_ingestion_ts` per key so MERGE always sees ≤1 source
    row per target row.
"""

from __future__ import annotations

import argparse

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window, functions as F


EVENT_TABLES = ("orders", "order_items", "order_payments", "order_reviews")

NATURAL_KEYS: dict[str, list[str]] = {
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "order_payments": ["order_id", "payment_sequential"],
    "order_reviews": ["review_id"],
}


def conform_orders(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp"))
        .withColumn("order_approved_at", F.to_timestamp("order_approved_at"))
        .withColumn("order_delivered_carrier_date", F.to_timestamp("order_delivered_carrier_date"))
        .withColumn("order_delivered_customer_date", F.to_timestamp("order_delivered_customer_date"))
        .withColumn("order_estimated_delivery_date", F.to_timestamp("order_estimated_delivery_date"))
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
        df.withColumn("price", F.col("price").cast("decimal(10,2)"))
        .withColumn("freight_value", F.col("freight_value").cast("decimal(10,2)"))
        .withColumn("shipping_limit_date", F.to_timestamp("shipping_limit_date"))
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
        .withColumn("payment_value", F.col("payment_value").cast("decimal(10,2)"))
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
        df.withColumn("review_creation_date", F.to_timestamp("review_creation_date"))
        .withColumn("review_answer_timestamp", F.to_timestamp("review_answer_timestamp"))
        .withColumn("review_score", F.col("review_score").cast("int"))
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


def dedupe_by_key(
    df: DataFrame, keys: list[str], order_col: str = "_ingestion_ts"
) -> DataFrame:
    """Keeps the latest row per natural key, ranked by `order_col` desc.

    Required before MERGE: without it, a microbatch with multiple rows for
    the same key raises MULTI_SOURCE_ROW_MATCH_FOR_TARGET.
    """
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def upsert_to_silver(spark: SparkSession, target_table: str, table_name: str):
    """foreachBatch callback: dedupe → conform → MERGE."""
    keys = NATURAL_KEYS[table_name]
    conform_fn = CONFORMERS[table_name]
    merge_condition = " AND ".join(f"t.{k} = s.{k}" for k in keys)

    def _upsert(batch_df: DataFrame, batch_id: int) -> None:
        deduped = dedupe_by_key(batch_df, keys)
        conformed = conform_fn(deduped)
        print(f"[silver] {table_name:14s} batch {batch_id}")

        if not spark.catalog.tableExists(target_table):
            conformed.write.format("delta").saveAsTable(target_table)
            return

        delta_tbl = DeltaTable.forName(spark, target_table)
        (
            delta_tbl.alias("t")
            .merge(conformed.alias("s"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    return _upsert


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--table", required=True, choices=EVENT_TABLES)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    source_table = f"{args.catalog}.bronze.{args.table}"
    target_table = f"{args.catalog}.silver.fact_{args.table}"
    checkpoint = f"/Volumes/{args.catalog}/_checkpoints/silver_fact_{args.table}"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.silver")

    (
        spark.readStream.table(source_table)
        .writeStream.foreachBatch(upsert_to_silver(spark, target_table, args.table))
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )

    print(f"[silver] {args.table:14s} streamed MERGE into {target_table}")


if __name__ == "__main__":
    main()
