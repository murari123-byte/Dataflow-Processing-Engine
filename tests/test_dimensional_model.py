
import pytest
from pyspark.sql import SparkSession

from src.processing.dimensional_model import build_dimensional_model


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("YottaFlexDimensionalModelTests")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


@pytest.fixture
def sample_data(spark):
    enriched_orders = spark.createDataFrame(
        [
            ("O001", "C001", "P001", "2025-01-01", 2, 100.0, 200.0, "orders.csv"),
            ("O002", "C002", "P002", "2025-01-02", 1, 200.0, 200.0, "orders.csv"),
            ("O003", "C001", "P001", "2025-01-03", 3, 50.0, 150.0, "orders.csv"),
        ],
        """
        order_id STRING,
        customer_id STRING,
        product_id STRING,
        order_date STRING,
        quantity INT,
        unit_price DOUBLE,
        total_amount DOUBLE,
        source_file STRING
        """
    )

    customers = spark.createDataFrame(
        [
            ("C001", "Customer One", "Hyderabad", "2024-01-01"),
            ("C002", "Customer Two", "Bengaluru", "2024-02-01"),
        ],
        """
        customer_id STRING,
        customer_name STRING,
        city STRING,
        signup_date STRING
        """
    )

    return enriched_orders, customers


def test_dimensional_model_record_counts(spark, sample_data):
    enriched_orders, customers = sample_data

    fact_orders, dim_customers, dim_products = (
        build_dimensional_model(enriched_orders, customers)
    )

    assert fact_orders.count() == 3
    assert dim_customers.count() == 2
    assert dim_products.count() == 2


def test_fact_orders_has_unique_order_ids(sample_data):
    enriched_orders, customers = sample_data

    fact_orders, _, _ = build_dimensional_model(
        enriched_orders, customers
    )

    assert fact_orders.filter(
        "order_id IS NULL"
    ).count() == 0

    assert (
        fact_orders.count()
        == fact_orders.select("order_id").distinct().count()
    )


def test_customer_dimension_has_unique_ids(sample_data):
    enriched_orders, customers = sample_data

    _, dim_customers, _ = build_dimensional_model(
        enriched_orders, customers
    )

    assert dim_customers.filter(
        "customer_id IS NULL"
    ).count() == 0

    assert (
        dim_customers.count()
        == dim_customers.select("customer_id").distinct().count()
    )


def test_product_dimension_has_unique_ids(sample_data):
    enriched_orders, customers = sample_data

    _, _, dim_products = build_dimensional_model(
        enriched_orders, customers
    )

    assert dim_products.filter(
        "product_id IS NULL"
    ).count() == 0

    assert (
        dim_products.count()
        == dim_products.select("product_id").distinct().count()
    )


def test_fact_orders_customer_references_are_valid(
    spark, sample_data
):
    enriched_orders, customers = sample_data

    fact_orders, dim_customers, _ = build_dimensional_model(
        enriched_orders, customers
    )

    invalid_customers = (
        fact_orders
        .select("customer_id")
        .distinct()
        .join(
            dim_customers.select("customer_id"),
            on="customer_id",
            how="left_anti"
        )
    )

    assert invalid_customers.count() == 0


def test_fact_orders_product_references_are_valid(
    spark, sample_data
):
    enriched_orders, customers = sample_data

    fact_orders, _, dim_products = build_dimensional_model(
        enriched_orders, customers
    )

    invalid_products = (
        fact_orders
        .select("product_id")
        .distinct()
        .join(
            dim_products.select("product_id"),
            on="product_id",
            how="left_anti"
        )
    )

    assert invalid_products.count() == 0


def test_fact_order_count_matches_enriched_orders(sample_data):
    enriched_orders, customers = sample_data

    fact_orders, _, _ = build_dimensional_model(
        enriched_orders, customers
    )

    assert fact_orders.count() == enriched_orders.count()


def test_empty_orders_produce_empty_fact_and_product_dimension(spark):
    empty_orders = spark.createDataFrame(
        [],
        """
        order_id STRING,
        customer_id STRING,
        product_id STRING,
        order_date STRING,
        quantity INT,
        unit_price DOUBLE,
        total_amount DOUBLE,
        source_file STRING
        """
    )

    customers = spark.createDataFrame(
        [
            ("C001", "Customer One", "Hyderabad", "2024-01-01"),
        ],
        """
        customer_id STRING,
        customer_name STRING,
        city STRING,
        signup_date STRING
        """
    )

    fact_orders, dim_customers, dim_products = (
        build_dimensional_model(empty_orders, customers)
    )

    assert fact_orders.count() == 0
    assert dim_products.count() == 0
    assert dim_customers.count() == 1
