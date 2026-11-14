
from pyspark.sql import SparkSession

from src.config import (
    SPARK_MASTER,
    DAILY_SALES_PATH,
    REJECTED_ORDERS_PATH,
    REPORTS_DIR,
)

from src.processing.analytics import run_analytics


def main():
    spark = None

    try:
        spark = (
            SparkSession.builder
            .master(SPARK_MASTER)
            .appName("YottaFlex-Analytics")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("ERROR")

        print("\n===== YOTTAFLEX ANALYTICS =====")

        # Read the published pipeline outputs.
        print("\nReading published datasets...")

        daily_sales = spark.read.parquet(str(DAILY_SALES_PATH))
        rejected_orders = spark.read.parquet(str(REJECTED_ORDERS_PATH))

        # Execute analytical queries.
        print("\nExecuting analytical queries...")

        results = run_analytics(
            spark=spark,
            daily_sales=daily_sales,
            rejected_orders=rejected_orders,
        )

        # Save each query result separately.
        analytics_dir = REPORTS_DIR / "analytics"
        analytics_dir.mkdir(parents=True, exist_ok=True)

        for query_name, result_df in results.items():
            print(f"\n===== {query_name.upper()} =====")

            result_df.show(20, truncate=False)

            output_path = analytics_dir / query_name

            result_df.coalesce(1).write \
                .mode("overwrite") \
                .option("header", True) \
                .csv(str(output_path))

            print("Saved:", output_path)

        print("\nAll analytical queries completed successfully.")

    except Exception as error:
        print("\nAnalytics execution failed:", str(error))
        raise

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
