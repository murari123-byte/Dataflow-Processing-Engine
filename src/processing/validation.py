
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


REQUIRED_ORDER_COLUMNS = {
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "order_ts",
}

REQUIRED_CUSTOMER_COLUMNS = {
    "customer_id",
}


def validate_schema(df: DataFrame, required_columns: set, name: str):
    """Fail fast when required columns are missing."""

    missing = required_columns - set(df.columns)

    if missing:
        raise ValueError(
            f"{name} schema is missing required columns: {sorted(missing)}"
        )


def validate_orders(
    orders: DataFrame,
    customers: DataFrame,
    source_file: str = "orders.csv",
):
    """
    Validate orders and separate valid and rejected records.

    Returns:
        valid_orders: Clean, typed records.
        rejected_orders: Original records with rejection reasons.
    """

    validate_schema(orders, REQUIRED_ORDER_COLUMNS, "Orders")
    validate_schema(customers, REQUIRED_CUSTOMER_COLUMNS, "Customers")

    # Preserve original input columns for quarantine.
    df = orders.withColumn(
        "source_file",
        F.lit(source_file)
    )

    # Parse and cast values safely.
    df = (
        df
        .withColumn("quantity_parsed", F.col("quantity").cast("int"))
        .withColumn("price_parsed", F.col("unit_price").cast("double"))
        .withColumn("timestamp_parsed", F.to_timestamp("order_ts"))
    )

    # Count occurrences of each order ID.
    duplicate_window = Window.partitionBy("order_id")

    df = df.withColumn(
        "order_id_count",
        F.count("*").over(duplicate_window)
    )

    # Check whether customer references exist.
    customer_lookup = (
        customers
        .select("customer_id")
        .dropDuplicates()
        .withColumn("_customer_exists", F.lit(True))
    )

    df = df.join(customer_lookup, on="customer_id", how="left")

    # Build explicit rejection reasons.
    df = df.withColumn(
        "reject_reason",
        F.concat_ws(
            "; ",
            F.when(
                F.col("order_id").isNull() |
                (F.trim(F.col("order_id")) == ""),
                F.lit("missing_order_id")
            ),
            F.when(
                F.col("customer_id").isNull() |
                (F.trim(F.col("customer_id")) == ""),
                F.lit("missing_customer_id")
            ),
            F.when(
                F.col("_customer_exists").isNull() &
                F.col("customer_id").isNotNull(),
                F.lit("invalid_customer_reference")
            ),
            F.when(
                F.col("product_id").isNull() |
                (F.trim(F.col("product_id")) == ""),
                F.lit("missing_product_id")
            ),
            F.when(
                F.col("quantity_parsed").isNull(),
                F.lit("invalid_or_missing_quantity")
            ),
            F.when(
                F.col("quantity_parsed") <= 0,
                F.lit("non_positive_quantity")
            ),
            F.when(
                F.col("price_parsed").isNull(),
                F.lit("invalid_or_missing_unit_price")
            ),
            F.when(
                F.col("price_parsed") < 0,
                F.lit("negative_unit_price")
            ),
            F.when(
                F.col("timestamp_parsed").isNull(),
                F.lit("invalid_or_missing_timestamp")
            ),
            F.when(
                F.col("order_id_count") > 1,
                F.lit("duplicate_order_id")
            ),
        )
    )

    # Separate rejected records.
    rejected_orders = (
        df
        .filter(F.col("reject_reason") != "")
        .drop(
            "quantity_parsed",
            "price_parsed",
            "timestamp_parsed",
            "order_id_count",
            "_customer_exists",
        )
    )

    # Select only clean records.
    valid_orders = (
        df
        .filter(F.col("reject_reason") == "")
        .select(
            "order_id",
            "customer_id",
            "product_id",
            F.col("quantity_parsed").alias("quantity"),
            F.col("price_parsed").alias("unit_price"),
            F.col("timestamp_parsed").alias("order_ts"),
            "source_file",
        )
    )

    return valid_orders, rejected_orders
