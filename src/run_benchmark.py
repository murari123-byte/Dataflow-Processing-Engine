
import csv
import os
import statistics
import time

from pyspark.sql import SparkSession

from src.processing.validation import validate_orders
from src.processing.baseline import baseline_transform
from src.processing.optimized import optimized_transform


DATA_PATH = "data/generated/large"
REPORT_PATH = "reports/benchmark_results.csv"
REPETITIONS = 3


def run_baseline(valid_orders):
    start = time.perf_counter()

    _, daily_sales = baseline_transform(valid_orders)
    results = daily_sales.collect()

    elapsed = time.perf_counter() - start
    return results, elapsed


def run_optimized(valid_orders):
    start = time.perf_counter()

    enriched_orders, daily_sales = optimized_transform(valid_orders)
    results = daily_sales.collect()

    elapsed = time.perf_counter() - start

    enriched_orders.unpersist()
    return results, elapsed



def compare_results(baseline_rows, optimized_rows):
    """Compare results while allowing minor floating-point differences."""

    baseline_map = {
        (row.order_date, row.product_id): row
        for row in baseline_rows
    }

    optimized_map = {
        (row.order_date, row.product_id): row
        for row in optimized_rows
    }

    if baseline_map.keys() != optimized_map.keys():
        return False

    tolerance = 0.02

    for key in baseline_map:
        b = baseline_map[key]
        o = optimized_map[key]

        if b.total_orders != o.total_orders:
            return False

        if b.total_quantity != o.total_quantity:
            return False

        if abs(
            float(b.total_revenue) - float(o.total_revenue)
        ) > tolerance:
            return False

        if abs(
            float(b.average_order_value)
            - float(o.average_order_value)
        ) > tolerance:
            return False

    return True


def save_report(baseline_times, optimized_times, valid_count,
                rejected_count, group_count, improvement):
    os.makedirs("reports", exist_ok=True)

    with open(REPORT_PATH, "w", newline="") as file:
        writer = csv.writer(file)

        writer.writerow([
            "run",
            "baseline_seconds",
            "optimized_seconds"
        ])

        for index in range(REPETITIONS):
            writer.writerow([
                index + 1,
                round(baseline_times[index], 3),
                round(optimized_times[index], 3)
            ])

        writer.writerow([])
        writer.writerow(["metric", "value"])
        writer.writerow(["valid_records", valid_count])
        writer.writerow(["rejected_records", rejected_count])
        writer.writerow(["aggregation_groups", group_count])
        writer.writerow([
            "baseline_median_seconds",
            round(statistics.median(baseline_times), 3)
        ])
        writer.writerow([
            "optimized_median_seconds",
            round(statistics.median(optimized_times), 3)
        ])
        writer.writerow([
            "median_improvement_percent",
            round(improvement, 2)
        ])


def main():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("YottaFlex-Repeatable-Benchmark")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("Reading input datasets...")

        orders = (
            spark.read
            .option("header", True)
            .csv(f"{DATA_PATH}/orders.csv")
        )

        customers = (
            spark.read
            .option("header", True)
            .csv(f"{DATA_PATH}/customers.csv")
        )

        print("Validating records...")

        valid_orders, rejected_orders = validate_orders(
            orders, customers
        )

        valid_orders = valid_orders.cache()

        valid_count = valid_orders.count()
        rejected_count = rejected_orders.count()

        print("\nWarming up both transformations...")

        warmup_baseline, _ = run_baseline(valid_orders)
        warmup_optimized, _ = run_optimized(valid_orders)

        results_match = compare_results(
            warmup_baseline, warmup_optimized
        )

        if not results_match:
            print("RESULT CHECK: FAILED")
            return

        print("Warm-up comparison passed.")

        baseline_times = []
        optimized_times = []

        print(f"\nRunning {REPETITIONS} measured iterations...")

        for iteration in range(REPETITIONS):
            print(f"\nIteration {iteration + 1}")

            # Alternate execution order to reduce order bias.
            if iteration % 2 == 0:
                baseline_rows, baseline_time = run_baseline(
                    valid_orders
                )
                optimized_rows, optimized_time = run_optimized(
                    valid_orders
                )
            else:
                optimized_rows, optimized_time = run_optimized(
                    valid_orders
                )
                baseline_rows, baseline_time = run_baseline(
                    valid_orders
                )

            baseline_times.append(baseline_time)
            optimized_times.append(optimized_time)

            print(f"Baseline: {baseline_time:.3f} seconds")
            print(f"Optimized: {optimized_time:.3f} seconds")

            if not compare_results(baseline_rows, optimized_rows):
                results_match = False
                print("Output comparison failed.")
                break

        print("\n========== FINAL RESULTS ==========")

        print("Valid records:", valid_count)
        print("Rejected records:", rejected_count)

        if results_match:
            print("RESULT CHECK: PASSED")
        else:
            print("RESULT CHECK: FAILED")

        if results_match and len(baseline_times) == REPETITIONS:
            baseline_median = statistics.median(baseline_times)
            optimized_median = statistics.median(optimized_times)

            print(f"Baseline median: {baseline_median:.3f} seconds")
            print(f"Optimized median: {optimized_median:.3f} seconds")

            if baseline_median > 0:
                improvement = (
                    (baseline_median - optimized_median)
                    / baseline_median
                ) * 100
            else:
                improvement = 0

            print(f"Median improvement: {improvement:.2f}%")

            save_report(
                baseline_times,
                optimized_times,
                valid_count,
                rejected_count,
                len(baseline_rows),
                improvement
            )

            print(f"\nReport saved to: {REPORT_PATH}")

        valid_orders.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
