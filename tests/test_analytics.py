
from datetime import date

import pytest
from pyspark.sql import SparkSession

from src.processing.analytics import run_analytics


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("YottaFlex-Analytics-Tests")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


@pytest.fixture
def sample_data(spark):
    daily_sales = spark.createDataFrame(
        [
            (date(2025, 1, 1), "P001", 2, 4, 40.0, 20.0),
            (date(2025, 1, 2), "P001", 1, 2, 30.0, 30.0),
            (date(2025, 1, 1), "P002", 1, 2, 50.0, 50.0),
        ],
        [
            "order_date",
            "product_id",
            "total_orders",
            "total_quantity",
            "total_revenue",
            "average_order_value",
        ],
    )

    rejected_orders = spark.createDataFrame(
        [
            ("missing_customer_id; invalid_customer_reference",),
            ("negative_unit_price",),
            ("missing_customer_id",),
        ],
        ["reject_reason"],
    )

    return daily_sales, rejected_orders


def test_revenue_by_product(spark, sample_data):
    daily_sales, rejected_orders = sample_data

    results = run_analytics(
        spark, daily_sales, rejected_orders
    )

    rows = results["revenue_by_product"].collect()

    assert len(rows) == 2

    # P001: 40 + 30 = 70
    # P002: 50
    assert rows[0]["product_id"] == "P001"
    assert rows[0]["total_orders"] == 3
    assert rows[0]["total_quantity"] == 6
    assert rows[0]["total_revenue"] == pytest.approx(70.0)

    assert rows[1]["product_id"] == "P002"
    assert rows[1]["total_revenue"] == pytest.approx(50.0)


def test_daily_revenue(spark, sample_data):
    daily_sales, rejected_orders = sample_data

    results = run_analytics(
        spark, daily_sales, rejected_orders
    )

    rows = results["daily_revenue"].collect()

    assert len(rows) == 2

    # January 1: 40 + 50 = 90
    assert rows[0]["order_date"] == date(2025, 1, 1)
    assert rows[0]["total_orders"] == 3
    assert rows[0]["daily_revenue"] == pytest.approx(90.0)

    # January 2: 30
    assert rows[1]["order_date"] == date(2025, 1, 2)
    assert rows[1]["total_orders"] == 1
    assert rows[1]["daily_revenue"] == pytest.approx(30.0)


def test_top_10_products(spark, sample_data):
    daily_sales, rejected_orders = sample_data

    # Create 12 products with different revenue values.
    product_data = [
        (
            date(2025, 1, 1),
            f"P{i:03d}",
            1,
            1,
            float(i * 100),
            float(i * 100),
        )
        for i in range(1, 13)
    ]

    large_product_dataset = spark.createDataFrame(
        product_data,
        daily_sales.schema,
    )

    results = run_analytics(
        spark, large_product_dataset, rejected_orders
    )

    rows = results["top_10_products"].collect()

    assert len(rows) == 10
    assert rows[0]["product_id"] == "P012"
    assert rows[0]["total_revenue"] == pytest.approx(1200.0)
    assert rows[-1]["product_id"] == "P003"


def test_sales_summary(spark, sample_data):
    daily_sales, rejected_orders = sample_data

    results = run_analytics(
        spark, daily_sales, rejected_orders
    )

    summary = results["sales_summary"].first()

    assert summary["total_orders"] == 4
    assert summary["total_quantity"] == 8
    assert summary["total_revenue"] == pytest.approx(120.0)
    assert summary["average_order_value"] == pytest.approx(30.0)


def test_rejection_summary(spark, sample_data):
    daily_sales, rejected_orders = sample_data

    results = run_analytics(
        spark, daily_sales, rejected_orders
    )

    rows = results["rejection_summary"].collect()

    reason_counts = {
        row["individual_reason"]: row["rejected_record_count"]
        for row in rows
    }

    assert reason_counts["missing_customer_id"] == 2
    assert reason_counts["invalid_customer_reference"] == 1
    assert reason_counts["negative_unit_price"] == 1

    # One record contains two reasons, so there are
    # four reason occurrences across three rejected records.
    assert sum(reason_counts.values()) == 4
