import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path


def generate_data(output_dir, customers_count, orders_count, seed):
    random.seed(seed)

    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)

    customers_file = output / "customers.csv"
    orders_file = output / "orders.csv"
    manifest_file = output / "anomaly_manifest.csv"

    cities = ["Hyderabad", "Bengaluru", "Chennai", "Mumbai", "Delhi"]
    products = ["P001", "P002", "P003", "P004", "P005"]

    # Generate customers
    customers = []

    with customers_file.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["customer_id", "customer_name", "city", "signup_date"])

        for i in range(1, customers_count + 1):
            customer_id = f"C{i:06d}"
            name = f"Customer_{i}"
            city = random.choice(cities)
            signup_date = (
                datetime(2025, 1, 1) +
                timedelta(days=random.randint(0, 300))
            ).date().isoformat()

            customers.append(customer_id)
            writer.writerow([customer_id, name, city, signup_date])

    # Generate orders
    anomalies = []
    generated_orders = []

    with orders_file.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow([
            "order_id", "customer_id", "product_id",
            "quantity", "unit_price", "order_ts"
        ])

        for i in range(1, orders_count + 1):
            order_id = f"O{i:09d}"
            customer_id = random.choice(customers)
            product_id = random.choice(products)
            quantity = random.randint(1, 5)
            unit_price = round(random.uniform(100, 5000), 2)

            order_ts = (
                datetime(2025, 1, 1) +
                timedelta(days=random.randint(0, 365))
            ).isoformat()

            record = [
                order_id, customer_id, product_id,
                quantity, unit_price, order_ts
            ]

            # Introduce controlled anomalies
            if i % 1000 == 0:
                record[1] = None
                anomalies.append([order_id, "missing_customer_id"])

            elif i % 1000 == 100:
                record[1] = "INVALID_CUSTOMER"
                anomalies.append([order_id, "invalid_customer_reference"])

            elif i % 1000 == 200:
                record[3] = -1
                anomalies.append([order_id, "negative_quantity"])

            elif i % 1000 == 300:
                record[4] = -100
                anomalies.append([order_id, "negative_unit_price"])

            elif i % 1000 == 400:
                record[5] = "invalid_timestamp"
                anomalies.append([order_id, "malformed_timestamp"])

            writer.writerow(record)
            generated_orders.append(record)

            # Add a duplicate order with the same primary key
            if i % 2000 == 0:
                writer.writerow(record)
                anomalies.append([order_id, "duplicate_order_id"])

    # Keep expected anomaly labels separate from input data
    with manifest_file.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["order_id", "expected_issue"])
        writer.writerows(anomalies)

    print("Synthetic data generated successfully!")
    print(f"Customers: {customers_count}")
    print(f"Orders before duplicates: {orders_count}")
    print(f"Expected anomalies: {len(anomalies)}")
    print(f"Output directory: {output.resolve()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--customers", type=int, default=10000)
    parser.add_argument("--orders", type=int, default=100000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", default="data/generated")

    args = parser.parse_args()

    if args.customers <= 0 or args.orders <= 0:
        parser.error("Customers and orders must be positive.")

    generate_data(
        args.output,
        args.customers,
        args.orders,
        args.seed
    )
