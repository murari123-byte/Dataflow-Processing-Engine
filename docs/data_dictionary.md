# Data Dictionary – YottaFlex Data Engineering Pipeline

## 1. Overview

This document describes the source datasets, published datasets, column definitions, data types, keys, relationships, and business rules used in the YottaFlex Data Engineering project.

The pipeline ingests customer and order data, validates records, transforms valid orders, builds a dimensional model, and publishes analytical datasets in Parquet format.

## 2. Source Datasets

### 2.1 Customers

**Source:** `customers.csv`

**Entity:** Customer

**Description:** Contains customer profile and registration information.

| Column | Data type | Description |
|---|---|---|
| customer_id | String | Unique customer identifier |
| customer_name | String | Customer name |
| city | String | Customer city |
| signup_date | String (source) | Customer registration date |

**Primary key:** `customer_id`

### 2.2 Orders

**Source:** `orders.csv`

**Entity:** Order

**Description:** Contains order transactions and product purchase information.

| Column | Data type | Description |
|---|---|---|
| order_id | String | Unique order identifier |
| customer_id | String | Reference to customer |
| product_id | String | Product identifier |
| quantity | String (source) | Number of units ordered |
| unit_price | String (source) | Price per unit |
| order_ts | String (source) | Order timestamp |

**Primary key:** `order_id`

**Foreign key:** `customer_id` references `customers.customer_id`.

## 3. Published Dimensional Model

### 3.1 fact_orders

**Location:** `data/output/fact_orders`

**Entity:** Order transaction

**Grain:** One row per valid order.

| Column | Data type | Description |
|---|---|---|
| order_id | String | Unique order identifier |
| customer_id | String | Customer reference |
| product_id | String | Product reference |
| order_date | Date | Date derived from order timestamp |
| quantity | Integer | Number of units ordered |
| unit_price | Double | Price per unit |
| total_amount | Double | Quantity multiplied by unit price |
| source_file | String | Original source file name |

**Primary key:** `order_id`

**Foreign keys:**
- `customer_id` references `dim_customers.customer_id`.
- `product_id` references `dim_products.product_id`.

**Record count:** 99,500

### 3.2 dim_customers

**Location:** `data/output/dim_customers`

**Entity:** Customer

**Grain:** One row per unique customer ID.

| Column | Data type | Description |
|---|---|---|
| customer_id | String | Unique customer identifier |
| customer_name | String | Customer name |
| city | String | Customer city |
| signup_date | Date | Customer registration date |

**Primary key:** `customer_id`

**Record count:** 10,000

### 3.3 dim_products

**Location:** `data/output/dim_products`

**Entity:** Product

**Grain:** One row per unique product ID.

| Column | Data type | Description |
|---|---|---|
| product_id | String | Unique product identifier |

**Primary key:** `product_id`

**Record count:** 5

**Limitation:** The source orders dataset contains product IDs but no product descriptions, names, categories, or pricing attributes beyond the transaction-level unit price. Therefore, this dimension currently contains only product identifiers.

## 4. Other Published Datasets

### 4.1 valid_orders

**Location:** `data/output/valid_orders`

**Grain:** One row per valid order.

| Column | Data type | Description |
|---|---|---|
| order_id | String | Unique order identifier |
| customer_id | String | Customer reference |
| product_id | String | Product identifier |
| quantity | Integer | Order quantity |
| unit_price | Double | Unit price |
| order_ts | Timestamp | Parsed order timestamp |
| source_file | String | Source file name |

**Record count:** 99,500

### 4.2 daily_sales

**Location:** `data/output/daily_sales`

**Grain:** One row per order date and product.

| Column | Data type | Description |
|---|---|---|
| order_date | Date | Order date |
| product_id | String | Product identifier |
| total_orders | Long | Number of orders |
| total_quantity | Long | Total units sold |
| total_revenue | Double | Total sales revenue |
| average_order_value | Double | Average revenue per order |

**Composite key:** `(order_date, product_id)`

**Record count:** 1,830

### 4.3 rejected_orders

**Location:** `data/quarantine/rejected_orders`

**Grain:** One row per rejected input record.

| Column | Data type | Description |
|---|---|---|
| customer_id | String | Original customer identifier |
| order_id | String | Original order identifier |
| product_id | String | Original product identifier |
| quantity | String | Original quantity value |
| unit_price | String | Original unit price |
| order_ts | String | Original timestamp |
| source_file | String | Source file name |
| reject_reason | String | Validation failure reason or reasons |

**Record count:** 550

Rejected records preserve their original input values for traceability.

## 5. Relationships

The dimensional model uses the following relationships:

- `dim_customers.customer_id` → `fact_orders.customer_id`
- `dim_products.product_id` → `fact_orders.product_id`

Each fact order references one customer and one product.

The daily sales dataset summarizes fact orders by order date and product.

## 6. Business Rules

1. Order IDs must be present and unique.
2. Customer IDs must be present and reference an existing customer.
3. Product IDs must be present.
4. Quantity must be a valid positive integer.
5. Unit price must be a valid non-negative numeric value.
6. Order timestamps must be valid.
7. Duplicate order records are rejected.
8. Valid records are transformed and published.
9. Invalid records are quarantined with rejection reasons.
10. Daily sales revenue is calculated from quantity multiplied by unit price.
11. Customer and product dimensions contain unique identifiers.

## 7. Data Quality and Reconciliation

Latest successful pipeline execution:

| Metric | Value |
|---|---:|
| Input orders | 100,050 |
| Valid orders | 99,500 |
| Rejected orders | 550 |
| Customer dimension | 10,000 |
| Product dimension | 5 |
| Fact orders | 99,500 |
| Daily sales groups | 1,830 |

Input reconciliation:

`Input records = Valid records + Rejected records`

`100050 = 99500 + 550`

Aggregation reconciliation:

`Fact order count = Valid order count`

`99500 = 99500`

Both reconciliation checks passed.

## 8. Storage Format

All published datasets are stored in Apache Parquet format.

Parquet provides:
- Columnar storage.
- Efficient analytical reads.
- Compression.
- Schema information.
- Compatibility with Apache Spark and other analytical tools.

Spark's physical Parquet schema currently marks the columns as nullable. Required fields and business constraints are enforced through pipeline validation rather than Parquet-level primary-key or foreign-key constraints.

