# Milestone 4: Distributed & Streaming Pipeline

[![Python CI Pipeline](https://github.com/Omjagtapp/ids568-milestone4-ojagt/actions/workflows/ci.yml/badge.svg)](https://github.com/Omjagtapp/ids568-milestone4-ojagt/actions)

## Repository Structure

```
ids568-milestone4-ojagt/
├── pipeline.py           # Distributed feature engineering (PySpark)
├── generate_data.py      # Synthetic data generation (10 M+ rows, seeded)
├── producer.py           # Kafka event producer with bursty traffic simulation
├── consumer.py           # Spark Structured Streaming consumer (exactly-once)
├── generate_plots.py     # Benchmark visualisation scripts
├── requirements.txt      # Python dependencies with pinned versions
├── README.md             # This file
├── Report.md             # Performance analysis & architecture evaluation
└── STREAMING_REPORT.md   # Streaming pipeline analysis & metrics
```

---

## Setup Instructions

### 1. Prerequisites

- Python 3.9+
- Java 11+ (required by PySpark)
- Apache Kafka 3.x (required for the streaming bonus only)

### 2. Create and activate a virtual environment

```bash
python -m venv venv
# macOS / Linux
source venv/bin/activate
# Windows
venv\Scripts\activate
```

### 3. Install Python dependencies

```bash
pip install -r requirements.txt
```

---

## Running the Pipeline

### Step 1 — Generate synthetic data (seeded for reproducibility)

```bash
python generate_data.py --rows 10000000 --output data/raw --seed 42
```

This produces 10 × 1 M-row Parquet chunks under `data/raw/`.
The `--seed 42` flag guarantees identical output on every machine.

**Reproducibility verification:**

```bash
python generate_data.py --rows 100 --seed 42 --output run1/
python generate_data.py --rows 100 --seed 42 --output run2/
diff -r run1/ run2/ && echo "Reproducible OK" || echo "MISMATCH"
rm -rf run1/ run2/
```

### Step 2 — Run the baseline (single-core) benchmark

```bash
python pipeline.py \
    --input  data/raw \
    --output data/features_local \
    --master local[1] \
    --partitions 10
```

### Step 3 — Run the distributed benchmark

```bash
python pipeline.py \
    --input  data/raw \
    --output data/features_dist \
    --master local[*] \
    --partitions 200
```

Both runs log `total_runtime_seconds` and `num_features_engineered` to MLflow.
View results in the MLflow UI:

```bash
mlflow ui   # open http://localhost:5000
```

### Step 4 — Generate performance visualisations

```bash
python generate_plots.py
```

Produces `runtime_vs_workers.png` and `throughput_vs_partitions.png`.

---

## Features Engineered

The pipeline builds **23 per-user features** across five stages:

| Stage | Features |
| :--- | :--- |
| Temporal | `hour_of_day`, `day_of_week`, `month`, `is_weekend` |
| Categorical encoding | `category_index` (StringIndexer), `is_electronics`, `is_groceries`, `is_clothing`, `is_entertainment` |
| Window functions | `prev_amount` (lag-1), `amount_delta`, `txn_rank` (row_number), `running_total` |
| Grouped aggregations | `total_spent`, `avg_transaction`, `transaction_count`, `max_transaction`, `min_transaction`, `stddev_transaction`, `avg_log_amount`, `avg_hour`, `avg_day_of_week`, `weekend_ratio`, `active_months`, `avg_category_index`, `avg_amount_delta`, `total_transactions_ranked`, `final_running_total` |
| Normalisation (z-score) | `total_spent_zscore`, `avg_transaction_zscore`, `txn_count_zscore` |

---

## Streaming Bonus

### Prerequisites

Start a local Kafka broker:

```bash
# Using Docker
docker run -d --name kafka \
  -p 9092:9092 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  apache/kafka:3.7.0
```

### Run the producer

```bash
python producer.py   # publishes to topic: mlops_transactions
```

The producer simulates steady-state and bursty traffic patterns.

### Run the consumer

```bash
python consumer.py
```

The consumer:
- Reads from `mlops_transactions` topic
- Deduplicates events by `event_id` (`dropDuplicates` inside `foreachBatch`)
- Persists Kafka offsets + state to `/tmp/spark_checkpoint_mlops`
- Aggregates over 10-second tumbling windows with a 10-second watermark

This combination of **checkpointing + deduplication** achieves **exactly-once** processing semantics.

---

## Sanity Checks

Run before submitting to verify everything works end-to-end:

```bash
# Syntax check all Python files
for f in *.py; do python -m py_compile "$f" && echo "OK $f"; done

# Quick pipeline smoke test (1,000 rows)
python generate_data.py --rows 1000 --output test_data/ --seed 42
python pipeline.py --input test_data/ --output test_output/ --master local[*] --partitions 10
echo "Exit code: $?"
rm -rf test_data/ test_output/

# Reproducibility check
python generate_data.py --rows 100 --seed 42 --output run1/
python generate_data.py --rows 100 --seed 42 --output run2/
diff -r run1/ run2/ && echo "Reproducible" || echo "NOT reproducible"
rm -rf run1/ run2/
```
