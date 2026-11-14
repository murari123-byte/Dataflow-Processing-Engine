
from pathlib import Path

# Project directories
PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"
GENERATED_DATA_DIR = DATA_DIR / "generated"
OUTPUT_DIR = DATA_DIR / "output"
QUARANTINE_DIR = DATA_DIR / "quarantine"

REPORTS_DIR = PROJECT_ROOT / "reports"

# Input datasets
INPUT_DATA_DIR = GENERATED_DATA_DIR / "large"

ORDERS_FILE = INPUT_DATA_DIR / "orders.csv"
CUSTOMERS_FILE = INPUT_DATA_DIR / "customers.csv"

# Output datasets
VALID_ORDERS_PATH = OUTPUT_DIR / "valid_orders"
DAILY_SALES_PATH = OUTPUT_DIR / "daily_sales"
REJECTED_ORDERS_PATH = QUARANTINE_DIR / "rejected_orders"

# Dimensional model outputs
FACT_ORDERS_PATH = OUTPUT_DIR / "fact_orders"
DIM_CUSTOMERS_PATH = OUTPUT_DIR / "dim_customers"
DIM_PRODUCTS_PATH = OUTPUT_DIR / "dim_products"

# Monitoring
RUN_LOG_PATH = REPORTS_DIR / "pipeline_runs.jsonl"

# Benchmark settings
BENCHMARK_REPORT_PATH = REPORTS_DIR / "benchmark_results.csv"
BENCHMARK_REPETITIONS = 3

# Spark settings
SPARK_MASTER = "local[*]"
SPARK_PARTITIONS = 8
RECONCILIATION_REPORT_PATH = REPORTS_DIR / "reconciliation_report.json"