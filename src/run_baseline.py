
from pyspark.sql import SparkSession
from src.processing.validation import validate_orders
from src.processing.baseline import baseline_transform

def main():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("YottaFlex-Baseline")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    base_path = "data/generated/large"

    orders = (
        spark.read
        .option("header", True)
        .csv(f"{base_path}/orders.csv")
    )

    customers = (
        spark.read
        .option("header", True)
        .csv(f"{base_path}/customers.csv")
    )

    valid_orders, rejected_orders = validate_orders(
        orders, customers
    )

    enriched_orders, daily_sales = baseline_transform(valid_orders)

    print("\n===== BASELINE RESULTS =====")
    print("Input orders:", orders.count())
    print("Valid orders:", valid_orders.count())
    print("Rejected orders:", rejected_orders.count())
    print("Enriched orders:", enriched_orders.count())
    print("Daily sales groups:", daily_sales.count())

    print("\n===== DAILY SALES SAMPLE =====")
    daily_sales.show(10, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
