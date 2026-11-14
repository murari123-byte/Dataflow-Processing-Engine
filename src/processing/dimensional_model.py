
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_dimensional_model(
    enriched_orders: DataFrame,
    customers: DataFrame,
):
    """
    Build a simple dimensional model.

    Fact grain: one row per valid order.
    Customer and product dimensions use natural source IDs.
    """

    # Customer dimension
    dim_customers = (
        customers
        .select(
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.col("customer_name"),
            F.col("city"),
            F.to_date(F.col("signup_date")).alias("signup_date"),
        )
        .filter(
            F.col("customer_id").isNotNull()
            & (F.col("customer_id") != "")
        )
        .dropDuplicates(["customer_id"])
    )

    # Product dimension
    dim_products = (
        enriched_orders
        .select("product_id")
        .filter(
            F.col("product_id").isNotNull()
            & (F.trim(F.col("product_id")) != "")
        )
        .dropDuplicates(["product_id"])
    )

    # Order fact table
    fact_orders = (
        enriched_orders
        .select(
            "order_id",
            "customer_id",
            "product_id",
            "order_date",
            "quantity",
            "unit_price",
            "total_amount",
            "source_file",
        )
    )

    return fact_orders, dim_customers, dim_products
