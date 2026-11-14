
import pytest
from pyspark.sql import SparkSession

from src.data import publisher


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("YottaFlexPipelineTests")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


@pytest.fixture
def sample_datasets(spark):
    valid_orders = spark.createDataFrame(
        [
            ("O001", "C001", "P001", 2, 100.0),
            ("O002", "C002", "P002", 1, 200.0),
        ],
        """
        order_id STRING,
        customer_id STRING,
        product_id STRING,
        quantity INT,
        unit_price DOUBLE
        """
    )

    daily_sales = spark.createDataFrame(
        [
            ("2025-01-01", "P001", 1, 2, 200.0),
            ("2025-01-02", "P002", 1, 1, 200.0),
        ],
        """
        order_date STRING,
        product_id STRING,
        total_orders LONG,
        total_quantity LONG,
        total_revenue DOUBLE
        """
    )

    rejected_orders = spark.createDataFrame(
        [
            (
                "O003", "C999", "P001", "1",
                "100.0", "2025-01-03",
                "orders.csv", "invalid_customer_reference"
            ),
        ],
        """
        order_id STRING,
        customer_id STRING,
        product_id STRING,
        quantity STRING,
        unit_price STRING,
        order_ts STRING,
        source_file STRING,
        reject_reason STRING
        """
    )

    fact_orders = spark.createDataFrame(
        [
            ("O001", "C001", "P001", "2025-01-01", 2, 100.0, 200.0, "orders.csv"),
            ("O002", "C002", "P002", "2025-01-02", 1, 200.0, 200.0, "orders.csv"),
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

    dim_customers = spark.createDataFrame(
        [
            ("C001", "Customer One", "Hyderabad"),
            ("C002", "Customer Two", "Bengaluru"),
        ],
        """
        customer_id STRING,
        customer_name STRING,
        city STRING
        """
    )

    dim_products = spark.createDataFrame(
        [
            ("P001",),
            ("P002",),
        ],
        ["product_id"]
    )

    return (
        valid_orders,
        daily_sales,
        rejected_orders,
        fact_orders,
        dim_customers,
        dim_products,
    )


def configure_output_paths(monkeypatch, tmp_path):
    output_dir = tmp_path / "output"
    quarantine_dir = tmp_path / "quarantine"

    paths = {
        "VALID_ORDERS_PATH": output_dir / "valid_orders",
        "DAILY_SALES_PATH": output_dir / "daily_sales",
        "REJECTED_ORDERS_PATH": quarantine_dir / "rejected_orders",
        "FACT_ORDERS_PATH": output_dir / "fact_orders",
        "DIM_CUSTOMERS_PATH": output_dir / "dim_customers",
        "DIM_PRODUCTS_PATH": output_dir / "dim_products",
    }

    for name, path in paths.items():
        monkeypatch.setattr(publisher, name, path)

    return paths


def publish_sample_datasets(sample_datasets):
    return publisher.publish_pipeline_outputs(*sample_datasets)


def test_all_pipeline_outputs_are_published(
    spark, sample_datasets, tmp_path, monkeypatch
):
    configure_output_paths(monkeypatch, tmp_path)

    paths = publish_sample_datasets(sample_datasets)

    expected_keys = {
        "valid_orders_path",
        "daily_sales_path",
        "rejected_orders_path",
        "fact_orders_path",
        "dim_customers_path",
        "dim_products_path",
    }

    assert set(paths.keys()) == expected_keys

    expected_counts = {
        "valid_orders_path": 2,
        "daily_sales_path": 2,
        "rejected_orders_path": 1,
        "fact_orders_path": 2,
        "dim_customers_path": 2,
        "dim_products_path": 2,
    }

    for key, expected_count in expected_counts.items():
        assert spark.read.parquet(
            paths[key]
        ).count() == expected_count


def test_overwrite_replaces_previous_output(spark, tmp_path):
    output_path = str(tmp_path / "overwrite_test")

    first_batch = spark.createDataFrame(
        [("O001",)],
        ["order_id"]
    )

    second_batch = spark.createDataFrame(
        [("O002",), ("O003",)],
        ["order_id"]
    )

    publisher.publish_parquet(first_batch, output_path)

    assert spark.read.parquet(output_path).count() == 1

    publisher.publish_parquet(second_batch, output_path)

    result = spark.read.parquet(output_path)

    assert result.count() == 2

    assert {
        row["order_id"] for row in result.collect()
    } == {"O002", "O003"}


def test_publishing_failure_is_propagated(
    sample_datasets, tmp_path, monkeypatch
):
    configure_output_paths(monkeypatch, tmp_path)

    def fail_publish(df, output_path):
        raise RuntimeError("Simulated publishing failure")

    monkeypatch.setattr(
        publisher,
        "publish_parquet",
        fail_publish
    )

    with pytest.raises(
        RuntimeError,
        match="Simulated publishing failure"
    ):
        publish_sample_datasets(sample_datasets)


def test_staging_failure_preserves_previous_outputs(
    spark, sample_datasets, tmp_path, monkeypatch
):
    configure_output_paths(monkeypatch, tmp_path)

    # Establish the previous successful outputs.
    paths = publish_sample_datasets(sample_datasets)

    original_publish = publisher.publish_parquet
    call_count = 0

    # Simulate a failure during the second staging operation.
    def fail_during_staging(df, output_path):
        nonlocal call_count
        call_count += 1

        if call_count == 2:
            raise RuntimeError("Simulated staging failure")

        original_publish(df, output_path)

    monkeypatch.setattr(
        publisher,
        "publish_parquet",
        fail_during_staging
    )

    with pytest.raises(
        RuntimeError,
        match="Simulated staging failure"
    ):
        publish_sample_datasets(sample_datasets)

    # All six previous outputs must remain unchanged.
    expected_counts = {
        "valid_orders_path": 2,
        "daily_sales_path": 2,
        "rejected_orders_path": 1,
        "fact_orders_path": 2,
        "dim_customers_path": 2,
        "dim_products_path": 2,
    }

    for key, expected_count in expected_counts.items():
        assert spark.read.parquet(
            paths[key]
        ).count() == expected_count

    # No staging directories should remain.
    for parent in (
        tmp_path / "output",
        tmp_path / "quarantine",
    ):
        assert not list(parent.glob(".*.staging.*"))



def test_commit_failure_restores_previous_outputs(
    spark, sample_datasets, tmp_path, monkeypatch
):
    configure_output_paths(monkeypatch, tmp_path)

    # First publish: create six previous successful outputs.
    paths = publish_sample_datasets(sample_datasets)

    original_replace = publisher.os.replace
    staging_commit_count = 0

    # Fail when the second staged dataset is being committed.
    def fail_second_staging_commit(source, destination):
        nonlocal staging_commit_count

        if ".staging." in str(source):
            staging_commit_count += 1

            if staging_commit_count == 2:
                raise OSError("Simulated commit failure")

        return original_replace(source, destination)

    monkeypatch.setattr(
        publisher.os,
        "replace",
        fail_second_staging_commit
    )

    with pytest.raises(
        OSError,
        match="Simulated commit failure"
    ):
        publish_sample_datasets(sample_datasets)

    # All six previous outputs must be restored or preserved.
    expected_counts = {
        "valid_orders_path": 2,
        "daily_sales_path": 2,
        "rejected_orders_path": 1,
        "fact_orders_path": 2,
        "dim_customers_path": 2,
        "dim_products_path": 2,
    }

    for key, expected_count in expected_counts.items():
        result = spark.read.parquet(paths[key])
        assert result.count() == expected_count

    # No staging or backup directories should remain.
    for parent in (
        tmp_path / "output",
        tmp_path / "quarantine",
    ):
        assert not list(parent.glob(".*.staging.*"))
        assert not list(parent.glob(".*.backup.*"))
