
import pytest

from pyspark.sql import SparkSession

from src.processing.reconciliation import reconcile_pipeline


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("YottaFlexReconciliationTests")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


def test_reconciliation_passes_for_matching_counts(spark):
    daily_sales = spark.createDataFrame(
        [
            ("2025-01-01", "P001", 2),
            ("2025-01-01", "P002", 3),
        ],
        """
        order_date STRING,
        product_id STRING,
        total_orders LONG
        """
    )

    result = reconcile_pipeline(
        input_count=10,
        valid_count=5,
        rejected_count=5,
        daily_sales=daily_sales,
    )

    assert result["input_reconciliation"] is True
    assert result["aggregation_reconciliation"] is True
    assert result["overall_status"] == "PASSED"


def test_reconciliation_fails_when_records_are_unclassified(spark):
    daily_sales = spark.createDataFrame(
        [("2025-01-01", "P001", 5)],
        """
        order_date STRING,
        product_id STRING,
        total_orders LONG
        """
    )

    with pytest.raises(RuntimeError, match="reconciliation failed"):
        reconcile_pipeline(
            input_count=10,
            valid_count=5,
            rejected_count=4,
            daily_sales=daily_sales,
        )


def test_reconciliation_fails_when_aggregation_loses_orders(spark):
    daily_sales = spark.createDataFrame(
        [("2025-01-01", "P001", 4)],
        """
        order_date STRING,
        product_id STRING,
        total_orders LONG
        """
    )

    with pytest.raises(RuntimeError, match="reconciliation failed"):
        reconcile_pipeline(
            input_count=10,
            valid_count=5,
            rejected_count=5,
            daily_sales=daily_sales,
        )


def test_reconciliation_handles_empty_aggregation(spark):
    daily_sales = spark.createDataFrame(
        [],
        """
        order_date STRING,
        product_id STRING,
        total_orders LONG
        """
    )

    result = reconcile_pipeline(
        input_count=0,
        valid_count=0,
        rejected_count=0,
        daily_sales=daily_sales,
    )

    assert result["aggregated_orders"] == 0
    assert result["overall_status"] == "PASSED"
