
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def reconcile_pipeline(
    input_count: int,
    valid_count: int,
    rejected_count: int,
    daily_sales: DataFrame,
):
    """
    Verify record counts across validation and aggregation.
    """

    # Check 1: Every input record must be valid or rejected.
    classified_count = valid_count + rejected_count

    input_check = input_count == classified_count

    # Check 2: Aggregated order count must equal valid order count.
    aggregation_result = daily_sales.agg(
        F.coalesce(
            F.sum("total_orders"),
            F.lit(0)
        ).alias("aggregated_orders")
    ).first()

    aggregated_orders = int(aggregation_result["aggregated_orders"])

    aggregation_check = aggregated_orders == valid_count

    results = {
        "input_records": input_count,
        "valid_records": valid_count,
        "rejected_records": rejected_count,
        "classified_records": classified_count,
        "aggregated_orders": aggregated_orders,
        "input_reconciliation": input_check,
        "aggregation_reconciliation": aggregation_check,
        "overall_status": (
            "PASSED"
            if input_check and aggregation_check
            else "FAILED"
        ),
    }

    print("\n===== DATA RECONCILIATION =====")

    print(
        f"Input records: {input_count} "
        f"(Valid: {valid_count} + Rejected: {rejected_count})"
    )

    print("Input reconciliation:", "PASSED" if input_check else "FAILED")

    print("Aggregated orders:", aggregated_orders)

    print(
        "Aggregation reconciliation:",
        "PASSED" if aggregation_check else "FAILED"
    )

    print("Overall reconciliation:", results["overall_status"])

    if results["overall_status"] == "FAILED":
        raise RuntimeError(
            f"Data reconciliation failed: {results}"
        )

    return results
