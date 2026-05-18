"""
Gold: Daily revenue aggregations
=================================

Reads from silver.orders + silver.order_items (joined upstream in a real
project; here we keep it focused on orders for the showcase). Produces:

  - gold.daily_orders: count of orders per day per status
  - gold.delivery_performance: avg/p95 days_to_delivery per week

These are the tables Soda Core then validates with business rules
(no negative counts, delivery time within plausible range, etc).
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    catalog = args.catalog
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")

    silver = spark.read.table(f"{catalog}.silver.orders")

    # ---- gold.daily_orders ---------------------------------------------------
    daily = (
        silver.withColumn("order_date", F.to_date("order_purchase_timestamp"))
        .groupBy("order_date", "order_status")
        .agg(F.count("order_id").alias("order_count"))
    )
    (
        daily.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.gold.daily_orders")
    )

    # ---- gold.delivery_performance ------------------------------------------
    weekly = (
        silver.filter(F.col("days_to_delivery").isNotNull())
        .withColumn("week_start", F.date_trunc("week", "order_purchase_timestamp"))
        .groupBy("week_start")
        .agg(
            F.avg("days_to_delivery").alias("avg_days_to_delivery"),
            F.expr("percentile(days_to_delivery, 0.95)").alias("p95_days_to_delivery"),
            F.count("order_id").alias("delivered_orders"),
        )
    )
    (
        weekly.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.gold.delivery_performance")
    )

    print("[gold] daily_orders + delivery_performance written")


if __name__ == "__main__":
    main()
