# Performance Benchmark Report

## 1. Objective

This report compares baseline and optimized Spark implementations to evaluate two performance techniques:

1. DataFrame partitioning and persistence for aggregation workloads.
2. Broadcast joins instead of sort-merge joins for customer reference lookups.

Both experiments use the generated dataset and compare output correctness before considering performance improvements.

## 2. Benchmark Environment

| Setting | Value |
|---|---|
| Apache Spark | 3.5.6 |
| Java | 11.0.27 |
| Execution environment | Docker on Windows with WSL2 |
| Spark master | Local mode |
| Input orders | 100,050 |
| Unique customers | 10,000 |
| Shuffle partitions | 8 |
| Benchmark repetitions | 3 |

These measurements represent local execution results, not a distributed cluster benchmark.

## 3. Aggregation Benchmark

### Baseline implementation

The baseline transformation:

- Derives `order_date` from `order_ts`.
- Calculates `total_amount` using quantity multiplied by unit price.
- Groups records by `order_date` and `product_id`.
- Calculates order count, total quantity, total revenue, and average order value.

### Optimized implementation

The optimized transformation performs the same calculations, with two additional techniques:

- Repartitions the enriched dataset using `order_date` and `product_id`.
- Persists the enriched DataFrame using `MEMORY_AND_DISK` because it is reused by the output and aggregation operations.

### Results

| Measurement | Baseline | Optimized |
|---|---:|---:|
| Median execution time | 3.259 sec | 0.982 sec |
| Improvement | | 69.86% |

The optimized implementation was faster in the measured benchmark.

Both implementations produced matching aggregation results. The benchmark checked grouping keys, order counts, quantities, revenue, and average order value.

**Interpretation:** Repartitioning and persistence improved the measured workload in this environment. Repartitioning itself introduces a shuffle, so this result should not be interpreted as proof that repartitioning always reduces shuffle operations. Its benefit depends on the workload and reuse pattern.

## 4. Join Strategy Benchmark

### Baseline implementation

The baseline uses a standard left join between orders and the deduplicated customer lookup.

For a controlled comparison, automatic broadcast selection was disabled:

`spark.sql.autoBroadcastJoinThreshold = -1`

The resulting physical plan used `SortMergeJoin`.

### Optimized implementation

The optimized version uses an explicit broadcast hint:

`F.broadcast(customer_lookup)`

The resulting physical plan used `BroadcastHashJoin`.

Adaptive Query Execution was disabled for this experiment so the planned join strategies could be compared directly.

### Results

| Iteration | Standard join | Broadcast join |
|---|---:|---:|
| 1 | 0.622 sec | 0.369 sec |
| 2 | 0.407 sec | 0.329 sec |
| 3 | 0.423 sec | 0.286 sec |
| **Median** | **0.423 sec** | **0.329 sec** |

**Measured median improvement: 22.06%**

### Physical plan observations

| Observation | Standard join | Broadcast join |
|---|---|---|
| Join operator | SortMergeJoin | BroadcastHashJoin |
| Orders-side join shuffle | Present | Avoided |
| Customer lookup deduplication shuffle | Present | Present |
| Broadcast exchange | Not used for the join | Used |

The customer lookup contains 10,000 unique customer IDs. Its deduplication requires a shuffle in both implementations. The broadcast join avoids repartitioning the large orders dataset for the join itself.

### Data correctness

Both strategies produced matching validation metrics:

| Metric | Result |
|---|---:|
| Input orders | 100,050 |
| Missing customer IDs | 150 |
| Invalid customer references | 100 |
| Output comparison | Passed |

## 5. Overall Comparison

| Experiment | Baseline median | Optimized median | Measured improvement |
|---|---:|---:|---:|
| Aggregation | 3.259 sec | 0.982 sec | 69.86% |
| Customer join | 0.423 sec | 0.329 sec | 22.06% |

The experiments show that choosing suitable partitioning, persistence, and join strategies can improve Spark workload performance.

## 6. Limitations and Trade-offs

- Results were collected in local Docker execution, not across a multi-node cluster.
- Only three measured iterations were used for each comparison.
- Runtime can vary due to caching, system load, and execution conditions.
- Repartitioning introduces a shuffle and may not benefit every workload.
- Broadcasting is suitable when the lookup dataset is small enough to fit in executor memory.
- The customer lookup preparation still requires deduplication and a shuffle.
- These results should be re-evaluated with larger datasets and distributed execution before production use.

## 7. Conclusion

The optimized aggregation and broadcast join implementations both passed their output comparisons and showed lower median execution times in the tested environment.

The measured improvements were 69.86% for aggregation and 22.06% for the customer join.

The benchmark supports using these techniques for the current workload while recognizing that the best strategy depends on dataset size, available memory, partitioning, and execution environment.
