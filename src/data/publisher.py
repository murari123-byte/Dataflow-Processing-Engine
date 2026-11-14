
import os
import shutil
import uuid

from pathlib import Path

from pyspark.sql import DataFrame

from src.config import (
    VALID_ORDERS_PATH,
    DAILY_SALES_PATH,
    REJECTED_ORDERS_PATH,
    FACT_ORDERS_PATH,
    DIM_CUSTOMERS_PATH,
    DIM_PRODUCTS_PATH,
)


def publish_parquet(df: DataFrame, output_path):
    """Write a DataFrame to a Parquet directory."""

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    (
        df.write
        .mode("overwrite")
        .parquet(str(output_path))
    )


def remove_path(path):
    """Remove a file or directory if it exists."""

    path = Path(path)

    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def publish_pipeline_outputs(
    valid_orders: DataFrame,
    daily_sales: DataFrame,
    rejected_orders: DataFrame,
    fact_orders: DataFrame,
    dim_customers: DataFrame,
    dim_products: DataFrame,
):
    """
    Stage all datasets before publishing.

    Restore previous outputs if a commit operation fails.
    """

    run_id = uuid.uuid4().hex

    datasets = [
        ("valid_orders", valid_orders, Path(VALID_ORDERS_PATH)),
        ("daily_sales", daily_sales, Path(DAILY_SALES_PATH)),
        ("rejected_orders", rejected_orders, Path(REJECTED_ORDERS_PATH)),
        ("fact_orders", fact_orders, Path(FACT_ORDERS_PATH)),
        ("dim_customers", dim_customers, Path(DIM_CUSTOMERS_PATH)),
        ("dim_products", dim_products, Path(DIM_PRODUCTS_PATH)),
    ]

    entries = []

    for name, dataframe, target in datasets:
        target.parent.mkdir(parents=True, exist_ok=True)

        entries.append({
            "name": name,
            "dataframe": dataframe,
            "target": target,
            "staging": target.parent / f".{target.name}.staging.{run_id}",
            "backup": target.parent / f".{target.name}.backup.{run_id}",
        })

    backed_up = []
    published = []

    try:
        # Phase 1: Write every dataset to staging.
        for entry in entries:
            print(f"\nStaging {entry['name']}...")

            publish_parquet(
                entry["dataframe"],
                entry["staging"],
            )

        # Phase 2: Commit staged datasets.
        for entry in entries:
            target = entry["target"]
            staging = entry["staging"]
            backup = entry["backup"]

            print(f"Publishing {entry['name']}...")

            if target.exists():
                os.replace(target, backup)
                backed_up.append(entry)

            os.replace(staging, target)
            published.append(entry)

        # Phase 3: Remove previous versions after success.
        for entry in backed_up:
            remove_path(entry["backup"])

        print("\nAll datasets published successfully.")

        return {
            "valid_orders_path": str(VALID_ORDERS_PATH),
            "daily_sales_path": str(DAILY_SALES_PATH),
            "rejected_orders_path": str(REJECTED_ORDERS_PATH),
            "fact_orders_path": str(FACT_ORDERS_PATH),
            "dim_customers_path": str(DIM_CUSTOMERS_PATH),
            "dim_products_path": str(DIM_PRODUCTS_PATH),
        }

    except Exception:
        print("\nPublishing failed. Restoring previous outputs...")

        # Remove any newly published datasets.
        for entry in reversed(published):
            remove_path(entry["target"])

        # Restore previous datasets.
        for entry in reversed(backed_up):
            backup = entry["backup"]
            target = entry["target"]

            if backup.exists():
                os.replace(backup, target)

        raise

    finally:
        # Remove leftover staging and backup directories.
        for entry in entries:
            remove_path(entry["staging"])

            if entry not in backed_up or entry["target"].exists():
                remove_path(entry["backup"])
