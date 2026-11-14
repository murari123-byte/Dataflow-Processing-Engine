
import json

from pyspark.sql import SparkSession

from src.config import (
    ORDERS_FILE,
    CUSTOMERS_FILE,
    SPARK_MASTER,
    REPORTS_DIR,
    RECONCILIATION_REPORT_PATH,
)
from src.processing.validation import validate_orders
from src.processing.optimized import optimized_transform
from src.processing.dimensional_model import build_dimensional_model
from src.processing.reconciliation import reconcile_pipeline
from src.data.publisher import publish_pipeline_outputs
from src.monitoring import start_run, finish_run


def main():
    run = start_run()
    spark = None

    input_count = 0
    valid_count = 0
    rejected_count = 0
    group_count = 0

    valid_orders = None
    enriched_orders = None

    try:
        spark = (
            SparkSession.builder
            .master(SPARK_MASTER)
            .appName("YottaFlex-Data-Pipeline")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("ERROR")

        print("\n===== YOTTAFLEX DATA PIPELINE =====")

        print("\nStep 1: Reading input datasets...")

        orders = (
            spark.read
            .option("header", True)
            .csv(str(ORDERS_FILE))
        )

        customers = (
            spark.read
            .option("header", True)
            .csv(str(CUSTOMERS_FILE))
        )

        print("\nStep 2: Validating records...")

        valid_orders, rejected_orders = validate_orders(
            orders, customers
        )

        valid_orders = valid_orders.cache()

        input_count = orders.count()
        valid_count = valid_orders.count()
        rejected_count = rejected_orders.count()

        print("Input records:", input_count)
        print("Valid records:", valid_count)
        print("Rejected records:", rejected_count)

        print("\nStep 3: Running optimized transformation...")

        enriched_orders, daily_sales = optimized_transform(
            valid_orders
        )

        group_count = daily_sales.count()

        print("Daily sales groups:", group_count)

        print("\nStep 4: Building dimensional model...")

        fact_orders, dim_customers, dim_products = (
            build_dimensional_model(
                enriched_orders,
                customers,
            )
        )

        fact_count = fact_orders.count()
        customer_dimension_count = dim_customers.count()
        product_dimension_count = dim_products.count()

        print("Fact order records:", fact_count)
        print("Customer dimension records:", customer_dimension_count)
        print("Product dimension records:", product_dimension_count)

        print("\nStep 5: Reconciling pipeline results...")

        reconciliation = reconcile_pipeline(
            input_count=input_count,
            valid_count=valid_count,
            rejected_count=rejected_count,
            daily_sales=daily_sales,
        )

        REPORTS_DIR.mkdir(parents=True, exist_ok=True)

        with open(
            RECONCILIATION_REPORT_PATH,
            "w",
            encoding="utf-8"
        ) as file:
            json.dump(reconciliation, file, indent=4)

        print(
            "\nReconciliation report saved:",
            RECONCILIATION_REPORT_PATH
        )

        print("\nStep 6: Publishing datasets...")

        output_paths = publish_pipeline_outputs(
            valid_orders=valid_orders,
            daily_sales=daily_sales,
            rejected_orders=rejected_orders,
            fact_orders=fact_orders,
            dim_customers=dim_customers,
            dim_products=dim_products,
        )

        print("\n===== PIPELINE SUMMARY =====")
        print("Pipeline status: SUCCESS")
        print("Input records:", input_count)
        print("Valid records:", valid_count)
        print("Rejected records:", rejected_count)
        print("Daily sales groups:", group_count)
        print("Fact order records:", fact_count)
        print("Customer dimension records:", customer_dimension_count)
        print("Product dimension records:", product_dimension_count)
        print("Reconciliation:", reconciliation["overall_status"])

        print("\nPublished locations:")

        for dataset, path in output_paths.items():
            print(f"{dataset}: {path}")

        record = finish_run(
            run,
            status="SUCCESS",
            input_records=input_count,
            valid_records=valid_count,
            rejected_records=rejected_count,
            aggregation_groups=group_count,
        )

        print("\nMonitoring record saved.")
        print("Run ID:", record["run_id"])
        print("Duration:", record["duration_seconds"], "seconds")

        print("\nPipeline completed successfully.")

    except Exception as error:
        record = finish_run(
            run,
            status="FAILED",
            input_records=input_count,
            valid_records=valid_count,
            rejected_records=rejected_count,
            aggregation_groups=group_count,
            error=error,
        )

        print("\nPIPELINE FAILED:", str(error))
        print("Failed run ID:", record["run_id"])

        raise

    finally:
        if enriched_orders is not None:
            enriched_orders.unpersist()

        if valid_orders is not None:
            valid_orders.unpersist()

        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
