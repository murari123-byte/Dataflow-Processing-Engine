import pytest
from pyspark.sql import SparkSession

from src.processing.validation import validate_orders


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("YottaFlexValidationTests")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


def make_customers(spark):
    return spark.createDataFrame(
        [("C001",), ("C002",)],
        ["customer_id"]
    )


def make_orders(spark, rows):
    return spark.createDataFrame(
        rows,
        [
            "order_id", "customer_id", "product_id",
            "quantity", "unit_price", "order_ts"
        ]
    )


def test_valid_and_invalid_records(spark):
    customers = make_customers(spark)

    orders = make_orders(spark, [
        ("O001", "C001", "P001", "2", "100.0", "2025-01-01T10:00:00"),
        ("O002", "C999", "P002", "1", "200.0", "2025-01-02T10:00:00"),
        ("O003", "C001", "P001", "-1", "100.0", "2025-01-03T10:00:00"),
        ("O004", "C002", "P001", "2", "100.0", "invalid_timestamp"),
        ("O005", None, "P002", "1", "200.0", "2025-01-05T10:00:00"),
    ])

    valid, rejected = validate_orders(orders, customers)

    assert valid.count() == 1
    assert rejected.count() == 4


def test_duplicate_order_ids_are_rejected(spark):
    customers = make_customers(spark)

    orders = make_orders(spark, [
        ("O001", "C001", "P001", "2", "100.0", "2025-01-01T10:00:00"),
        ("O001", "C001", "P002", "1", "200.0", "2025-01-02T10:00:00"),
        ("O002", "C001", "P001", "3", "150.0", "2025-01-03T10:00:00"),
    ])

    valid, rejected = validate_orders(orders, customers)

    assert valid.count() == 1
    assert rejected.count() == 2

    reasons = [r["reject_reason"] for r in rejected.collect()]
    assert all("duplicate_order_id" in reason for reason in reasons)


def test_empty_orders_input(spark):
    customers = make_customers(spark)

    empty_schema = """
        order_id STRING,
        customer_id STRING,
        product_id STRING,
        quantity STRING,
        unit_price STRING,
        order_ts STRING
    """

    orders = spark.createDataFrame([], empty_schema)

    valid, rejected = validate_orders(orders, customers)

    assert valid.count() == 0
    assert rejected.count() == 0


def test_missing_required_column_raises_error(spark):
    customers = make_customers(spark)

    orders = spark.createDataFrame(
        [("O001", "C001", "P001")],
        ["order_id", "customer_id", "product_id"]
    )

    with pytest.raises(ValueError, match="schema is missing required columns"):
        validate_orders(orders, customers)


def test_invalid_and_negative_prices_are_rejected(spark):
    customers = make_customers(spark)

    orders = make_orders(spark, [
        ("O001", "C001", "P001", "2", "invalid", "2025-01-01T10:00:00"),
        ("O002", "C001", "P001", "2", "-50.0", "2025-01-02T10:00:00"),
        ("O003", "C001", "P001", "2", "100.0", "2025-01-03T10:00:00"),
    ])

    valid, rejected = validate_orders(orders, customers)

    assert valid.count() == 1
    assert rejected.count() == 2

    reasons = {r["reject_reason"] for r in rejected.collect()}

    assert "invalid_or_missing_unit_price" in reasons
    assert "negative_unit_price" in reasons


def test_invalid_and_non_positive_quantities_are_rejected(spark):
    customers = make_customers(spark)

    orders = make_orders(spark, [
        ("O001", "C001", "P001", "abc", "100.0", "2025-01-01T10:00:00"),
        ("O002", "C001", "P001", "0", "100.0", "2025-01-02T10:00:00"),
        ("O003", "C001", "P001", "-2", "100.0", "2025-01-03T10:00:00"),
        ("O004", "C001", "P001", "3", "100.0", "2025-01-04T10:00:00"),
    ])

    valid, rejected = validate_orders(orders, customers)

    assert valid.count() == 1
    assert rejected.count() == 3

    reasons = {r["reject_reason"] for r in rejected.collect()}

    assert "invalid_or_missing_quantity" in reasons
    assert "non_positive_quantity" in reasons


def test_missing_ids_and_invalid_customer_reference(spark):
    customers = make_customers(spark)

    orders = make_orders(spark, [
        ("O001", None, "P001", "2", "100.0", "2025-01-01T10:00:00"),
        ("O002", "C999", "P001", "2", "100.0", "2025-01-02T10:00:00"),
        (None, "C001", "P001", "2", "100.0", "2025-01-03T10:00:00"),
    ])

    valid, rejected = validate_orders(orders, customers)

    assert valid.count() == 0
    assert rejected.count() == 3

    reasons = {r["reject_reason"] for r in rejected.collect()}

    assert "missing_customer_id" in reasons
    assert "invalid_customer_reference" in reasons
    assert "missing_order_id" in reasons


def test_missing_product_id_is_rejected(spark):
    customers = make_customers(spark)

    orders = make_orders(spark, [
        ("O001", "C001", None, "2", "100.0", "2025-01-01T10:00:00"),
        ("O002", "C001", "P001", "2", "100.0", "2025-01-02T10:00:00"),
    ])

    valid, rejected = validate_orders(orders, customers)

    assert valid.count() == 1
    assert rejected.count() == 1

    reason = rejected.select("reject_reason").first()["reject_reason"]
    assert "missing_product_id" in reason


def test_malformed_timestamps_are_rejected(spark):
    customers = make_customers(spark)

    orders = make_orders(spark, [
        ("O001", "C001", "P001", "2", "100.0", "not-a-date"),
        ("O002", "C001", "P001", "2", "100.0", "2025-01-02T10:00:00"),
    ])

    valid, rejected = validate_orders(orders, customers)

    assert valid.count() == 1
    assert rejected.count() == 1

    reason = rejected.select("reject_reason").first()["reject_reason"]
    assert "invalid_or_missing_timestamp" in reason


def test_null_values_are_rejected(spark):
    customers = make_customers(spark)

    orders = make_orders(spark, [
        ("O001", "C001", "P001", "2", None, "2025-01-01T10:00:00"),
        ("O002", "C001", "P002", None, "100.0", "2025-01-02T10:00:00"),
    ])

    valid, rejected = validate_orders(orders, customers)

    assert valid.count() == 0
    assert rejected.count() == 2

    reasons = {r["reject_reason"] for r in rejected.collect()}

    assert "invalid_or_missing_unit_price" in reasons
    assert "invalid_or_missing_quantity" in reasons



def test_multiple_rejection_reasons_are_recorded(spark):
    customers = make_customers(spark)

    orders = spark.createDataFrame(
        [
            ("O001", "C001", None, "-2", "100.0", "2025-01-01T10:00:00"),
        ],
        """
        order_id STRING,
        customer_id STRING,
        product_id STRING,
        quantity STRING,
        unit_price STRING,
        order_ts STRING
        """
    )

    valid, rejected = validate_orders(orders, customers)

    assert valid.count() == 0
    assert rejected.count() == 1

    reason = rejected.select("reject_reason").first()["reject_reason"]
    reason_set = set(reason.split("; "))

    assert "missing_product_id" in reason_set
    assert "non_positive_quantity" in reason_set
