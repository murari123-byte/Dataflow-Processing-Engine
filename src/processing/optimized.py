
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel


def optimized_transform(valid_orders: DataFrame):
    """
    Optimized transformation:
    1. Calculate revenue and order date.
    2. Repartition data using aggregation keys.
    3. Cache the prepared data for reuse.
    4. Aggregate daily sales by product.
    """

    enriched_orders = (
        valid_orders
        .withColumn("order_date", F.to_date(F.col("order_ts")))
        .withColumn(
            "total_amount",
            F.col("quantity") * F.col("unit_price")
        )
        .repartition(8, "order_date", "product_id")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    daily_sales = (
        enriched_orders
        .groupBy("order_date", "product_id")
        .agg(
            F.count("*").alias("total_orders"),
            F.sum("quantity").alias("total_quantity"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("average_order_value")
        )
    )

    return enriched_orders, daily_sales
