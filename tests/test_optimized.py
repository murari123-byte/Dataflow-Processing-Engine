
from datetime import date, datetime

import pytest
from pyspark.sql import SparkSession

from src.processing.optimized import optimized_transform


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("YottaFlexOptimizedTests")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


def test_out_of_order_orders_are_aggregated_correctly(spark):
    # Orders intentionally arrive out of chronological order.
    orders = spark.createDataFrame(
        [
            ("O001", "C001", "P001", 2, 100.0,
             datetime(2025, 1, 3, 10, 0, 0), "orders.csv"),

            ("O002", "C002", "P001", 1, 50.0,
             datetime(2025, 1, 1, 9, 0, 0), "orders.csv"),

            ("O003", "C003", "P001", 3, 20.0,
             datetime(2025, 1, 2, 11, 0, 0), "orders.csv"),

            ("O004", "C001", "P002", 1, 200.0,
             datetime(2025, 1, 1, 14, 0, 0), "orders.csv"),
        ],
        """
        order_id STRING,
        customer_id STRING,
        product_id STRING,
        quantity INT,
        unit_price DOUBLE,
        order_ts TIMESTAMP,
        source_file STRING
        """
    )

    enriched_orders, daily_sales = optimized_transform(orders)

    try:
        # All four input orders must be retained.
        assert enriched_orders.count() == 4

        results = {
            (row["order_date"], row["product_id"]): row
            for row in daily_sales.collect()
        }

        # January 1, P001
        jan_1_p001 = results[(date(2025, 1, 1), "P001")]
        assert jan_1_p001["total_orders"] == 1
        assert jan_1_p001["total_quantity"] == 1
        assert jan_1_p001["total_revenue"] == pytest.approx(50.0)

        # January 1, P002
        jan_1_p002 = results[(date(2025, 1, 1), "P002")]
        assert jan_1_p002["total_orders"] == 1
        assert jan_1_p002["total_revenue"] == pytest.approx(200.0)

        # January 2, P001
        jan_2_p001 = results[(date(2025, 1, 2), "P001")]
        assert jan_2_p001["total_orders"] == 1
        assert jan_2_p001["total_revenue"] == pytest.approx(60.0)

        # January 3, P001
        jan_3_p001 = results[(date(2025, 1, 3), "P001")]
        assert jan_3_p001["total_orders"] == 1
        assert jan_3_p001["total_revenue"] == pytest.approx(200.0)

        # Four distinct date/product groups.
        assert len(results) == 4

    finally:
        enriched_orders.unpersist()
