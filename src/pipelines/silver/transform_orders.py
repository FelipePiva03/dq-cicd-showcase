"""
Silver: Streaming MERGE into a conformed orders table
======================================================

Pattern: Structured Streaming (read from Bronze) + foreachBatch(MERGE INTO Silver).

Why foreachBatch instead of pure append:
  - Silver must be deduplicated by order_id and reflect status updates
    (delivered, canceled, etc). Plain `append` would leave us with multiple
    rows per order.
  - MERGE in foreachBatch gives us idempotent upserts: re-running a batch
    is safe.

Conformance done here:
  - Cast timestamps to proper types (Bronze inferred them as strings via CSV).
  - Normalize order_status to lowercase.
  - Drop ingestion-only columns the rest of the pipeline doesn't care about.
  - Add a derived `days_to_delivery` column for downstream Gold + DQ checks.
"""

from __future__ import annotations

import argparse

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    return parser.parse_args()


def conform(df: DataFrame) -> DataFrame:
    """Type-cast and normalize raw Bronze records into Silver schema."""
    return (
        df.withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp"))
        .withColumn("order_approved_at", F.to_timestamp("order_approved_at"))
        .withColumn("order_delivered_customer_date", F.to_timestamp("order_delivered_customer_date"))
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
            "order_delivered_customer_date",
            "days_to_delivery",
            F.current_timestamp().alias("_silver_processed_ts"),
        )
    )


def upsert_to_silver(spark: SparkSession, target_table: str):
    """Returns a foreachBatch function that MERGEs the micro-batch into Silver."""

    def _upsert(batch_df: DataFrame, batch_id: int) -> None:
        print(f"[silver] processing batch {batch_id} ({batch_df.count()} rows)")
        conformed = conform(batch_df)

        if not spark.catalog.tableExists(target_table):
            conformed.write.format("delta").saveAsTable(target_table)
            return

        delta_tbl = DeltaTable.forName(spark, target_table)
        (
            delta_tbl.alias("t")
            .merge(conformed.alias("s"), "t.order_id = s.order_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    return _upsert


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    catalog = args.catalog
    source_table = f"{catalog}.bronze.orders"
    target_table = f"{catalog}.silver.orders"
    checkpoint = f"/Volumes/{catalog}/_checkpoints/silver_orders"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")

    (
        spark.readStream.table(source_table)
        .writeStream.foreachBatch(upsert_to_silver(spark, target_table))
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )

    print(f"[silver] streamed MERGE into {target_table}")


if __name__ == "__main__":
    main()
