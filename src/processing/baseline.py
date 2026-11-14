
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def baseline_transform(valid_orders: DataFrame):
    """
    Baseline transformation:
    1. Calculate revenue for each order.
    2. Extract the order date.
    3. Aggregate daily sales by product.

    This version uses straightforward Spark transformations.
    """

    # Calculate revenue and extract order date
    enriched_orders = (
        valid_orders
        .withColumn(
            "order_date",
            F.to_date(F.col("order_ts"))
        )
        .withColumn(
            "total_amount",
            F.col("quantity") * F.col("unit_price")
        )
    )

    # Aggregate daily sales by product
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
