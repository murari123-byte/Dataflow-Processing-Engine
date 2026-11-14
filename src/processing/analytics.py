
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def run_analytics(
    spark: SparkSession,
    daily_sales: DataFrame,
    rejected_orders: DataFrame,
):
    """
    Run analytical queries against pipeline outputs.
    """

    daily_sales.createOrReplaceTempView("daily_sales")
    rejected_orders.createOrReplaceTempView("rejected_orders")

    queries = {}

    # Query 1: Revenue by product.
    queries["revenue_by_product"] = spark.sql("""
        SELECT
            product_id,
            SUM(total_orders) AS total_orders,
            SUM(total_quantity) AS total_quantity,
            ROUND(SUM(total_revenue), 2) AS total_revenue
        FROM daily_sales
        GROUP BY product_id
        ORDER BY total_revenue DESC
    """)

    # Query 2: Daily revenue trend.
    queries["daily_revenue"] = spark.sql("""
        SELECT
            order_date,
            SUM(total_orders) AS total_orders,
            ROUND(SUM(total_revenue), 2) AS daily_revenue
        FROM daily_sales
        GROUP BY order_date
        ORDER BY order_date
    """)

    # Query 3: Top 10 products by revenue.
    queries["top_10_products"] = spark.sql("""
        SELECT
            product_id,
            SUM(total_orders) AS total_orders,
            ROUND(SUM(total_revenue), 2) AS total_revenue
        FROM daily_sales
        GROUP BY product_id
        ORDER BY total_revenue DESC
        LIMIT 10
    """)

    # Query 4: Overall sales summary.
    queries["sales_summary"] = spark.sql("""
        SELECT
            SUM(total_orders) AS total_orders,
            SUM(total_quantity) AS total_quantity,
            ROUND(SUM(total_revenue), 2) AS total_revenue,
            ROUND(
                SUM(total_revenue) / NULLIF(SUM(total_orders), 0),
                2
            ) AS average_order_value
        FROM daily_sales
    """)

    # Query 5: Rejection reasons.
    # Split combined reasons so each individual reason is counted.
    rejection_reasons = (
        rejected_orders
        .withColumn(
            "individual_reason",
            F.explode(
                F.split(F.col("reject_reason"), r";\s*")
            )
        )
        .groupBy("individual_reason")
        .agg(F.count("*").alias("rejected_record_count"))
        .orderBy(F.desc("rejected_record_count"))
    )

    queries["rejection_summary"] = rejection_reasons

    return queries
