
import csv
import os
import statistics
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.config import (
    ORDERS_FILE,
    CUSTOMERS_FILE,
    SPARK_MASTER,
    SPARK_PARTITIONS,
)

REPORT_PATH = "reports/join_benchmark_results.csv"
REPETITIONS = 3


def prepare_customer_lookup(customers):
    """Create a unique customer lookup for reference validation."""
    return (
        customers
        .select("customer_id")
        .filter(
            F.col("customer_id").isNotNull()
            & (F.trim(F.col("customer_id")) != "")
        )
        .dropDuplicates()
        .withColumn("_customer_exists", F.lit(True))
        .cache()
    )


def standard_join(orders, customer_lookup):
    """Baseline: use a SortMergeJoin without broadcast optimization."""
    return orders.join(
        customer_lookup,
        on="customer_id",
        how="left"
    )


def broadcast_join(orders, customer_lookup):
    """Optimized: broadcast the smaller customer lookup."""
    return orders.join(
        F.broadcast(customer_lookup),
        on="customer_id",
        how="left"
    )


def collect_metrics(result):
    """Count records and distinguish missing from invalid references."""

    metrics = (
        result
        .agg(
            F.count("*").alias("total_records"),

            F.sum(
                F.when(
                    F.col("customer_id").isNull()
                    | (F.trim(F.col("customer_id")) == ""),
                    1
                ).otherwise(0)
            ).alias("missing_customer_ids"),

            F.sum(
                F.when(
                    F.col("customer_id").isNotNull()
                    & (F.trim(F.col("customer_id")) != "")
                    & F.col("_customer_exists").isNull(),
                    1
                ).otherwise(0)
            ).alias("invalid_customer_references")
        )
        .collect()[0]
    )

    return (
        metrics["total_records"],
        metrics["missing_customer_ids"] or 0,
        metrics["invalid_customer_references"] or 0
    )


def measure_join(join_function, orders, customer_lookup):
    """Execute and measure one join strategy."""

    start = time.perf_counter()

    result = join_function(orders, customer_lookup)

    metrics = collect_metrics(result)

    elapsed = time.perf_counter() - start

    return result, elapsed, metrics


def save_report(standard_times, broadcast_times, metrics):
    os.makedirs("reports", exist_ok=True)

    standard_median = statistics.median(standard_times)
    broadcast_median = statistics.median(broadcast_times)

    improvement = (
        (standard_median - broadcast_median)
        / standard_median * 100
        if standard_median > 0 else 0
    )

    with open(REPORT_PATH, "w", newline="") as file:
        writer = csv.writer(file)

        writer.writerow([
            "iteration",
            "standard_join_seconds",
            "broadcast_join_seconds"
        ])

        for index in range(REPETITIONS):
            writer.writerow([
                index + 1,
                round(standard_times[index], 3),
                round(broadcast_times[index], 3)
            ])

        writer.writerow([])
        writer.writerow(["metric", "value"])
        writer.writerow(["input_records", metrics[0]])
        writer.writerow(["missing_customer_ids", metrics[1]])
        writer.writerow(["invalid_customer_references", metrics[2]])
        writer.writerow([
            "standard_median_seconds",
            round(standard_median, 3)
        ])
        writer.writerow([
            "broadcast_median_seconds",
            round(broadcast_median, 3)
        ])
        writer.writerow([
            "improvement_percent",
            round(improvement, 2)
        ])

    return improvement


def main():
    spark = (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName("YottaFlex-Controlled-Join-Benchmark")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.shuffle.partitions", str(SPARK_PARTITIONS))
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("Reading input datasets...")

        orders = (
            spark.read
            .option("header", True)
            .csv(str(ORDERS_FILE))
            .cache()
        )

        customers = (
            spark.read
            .option("header", True)
            .csv(str(CUSTOMERS_FILE))
        )

        order_count = orders.count()

        print("Preparing customer lookup...")

        customer_lookup = prepare_customer_lookup(customers)
        customer_count = customer_lookup.count()

        print(f"Input orders: {order_count}")
        print(f"Unique customers: {customer_count}")
        print(f"Shuffle partitions: {SPARK_PARTITIONS}")

        print("\n========== STANDARD JOIN PLAN ==========")

        standard_plan = standard_join(orders, customer_lookup)
        standard_plan.explain(mode="formatted")

        print("\n========== BROADCAST JOIN PLAN ==========")

        broadcast_plan = broadcast_join(orders, customer_lookup)
        broadcast_plan.explain(mode="formatted")

        print("\nWarming up both join strategies...")

        standard_result, _, standard_metrics = measure_join(
            standard_join, orders, customer_lookup
        )

        broadcast_result, _, broadcast_metrics = measure_join(
            broadcast_join, orders, customer_lookup
        )

        if standard_metrics != broadcast_metrics:
            raise RuntimeError(
                "Join output comparison failed: metrics do not match."
            )

        print("Warm-up comparison passed.")

        standard_times = []
        broadcast_times = []

        print(f"\nRunning {REPETITIONS} measured iterations...")

        for iteration in range(REPETITIONS):
            print(f"\nIteration {iteration + 1}")

            # Alternate execution order to reduce measurement bias.
            if iteration % 2 == 0:
                _, standard_time, standard_metrics = measure_join(
                    standard_join, orders, customer_lookup
                )

                _, broadcast_time, broadcast_metrics = measure_join(
                    broadcast_join, orders, customer_lookup
                )
            else:
                _, broadcast_time, broadcast_metrics = measure_join(
                    broadcast_join, orders, customer_lookup
                )

                _, standard_time, standard_metrics = measure_join(
                    standard_join, orders, customer_lookup
                )

            if standard_metrics != broadcast_metrics:
                raise RuntimeError(
                    "Join output comparison failed."
                )

            standard_times.append(standard_time)
            broadcast_times.append(broadcast_time)

            print(f"Standard join: {standard_time:.3f} seconds")
            print(f"Broadcast join: {broadcast_time:.3f} seconds")
            print("Output comparison: PASSED")

        improvement = save_report(
            standard_times,
            broadcast_times,
            standard_metrics
        )

        print("\n========== FINAL RESULTS ==========")

        print(f"Input orders: {standard_metrics[0]}")
        print(f"Missing customer IDs: {standard_metrics[1]}")
        print(
            "Invalid customer references:",
            standard_metrics[2]
        )

        print(
            "Standard join median:",
            f"{statistics.median(standard_times):.3f} seconds"
        )

        print(
            "Broadcast join median:",
            f"{statistics.median(broadcast_times):.3f} seconds"
        )

        print(f"Median improvement: {improvement:.2f}%")
        print("Join output comparison: PASSED")
        print(f"Report saved to: {REPORT_PATH}")

        customer_lookup.unpersist()
        orders.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
