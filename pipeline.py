import argparse
import time
import mlflow
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.ml.feature import StringIndexer

def run_pipeline(input_path: str, output_path: str, master: str, partitions: int):
    print(f"Starting PySpark pipeline on master: {master} with {partitions} partitions...")

    mlflow.set_experiment("IDS568_Milestone4_FeatureEngineering")
    run_type = "Local_1_Core" if master == "local[1]" else "Distributed_Multi_Core"

    with mlflow.start_run(run_name=run_type):

        mlflow.log_param("master_config", master)
        mlflow.log_param("shuffle_partitions", partitions)

        spark = SparkSession.builder \
            .appName("DistributedFeatureEngineering") \
            .master(master) \
            .config("spark.sql.shuffle.partitions", partitions) \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        start_time = time.perf_counter()

        # Load Data
        df = spark.read.parquet(input_path)

        # =====================================================================
        # STEP 1: TEMPORAL FEATURE EXTRACTION
        # Extract time-based signals from each transaction's timestamp.
        # =====================================================================
        df = df.filter(F.col("amount") > 0) \
            .withColumn("timestamp", F.to_timestamp(F.col("timestamp"))) \
            .withColumn("hour_of_day",   F.hour(F.col("timestamp"))) \
            .withColumn("day_of_week",   F.dayofweek(F.col("timestamp"))) \
            .withColumn("month",         F.month(F.col("timestamp"))) \
            .withColumn("is_weekend",    (F.col("day_of_week").isin([1, 7])).cast("int"))

        # =====================================================================
        # STEP 2: CATEGORICAL ENCODING
        # Label-encode the 'category' column and create binary indicator columns
        # for each category (sparse one-hot representation).
        # =====================================================================
        indexer = StringIndexer(inputCol="category", outputCol="category_index", handleInvalid="keep")
        df = indexer.fit(df).transform(df)

        df = df \
            .withColumn("is_electronics",   (F.col("category") == "Electronics").cast("int")) \
            .withColumn("is_groceries",     (F.col("category") == "Groceries").cast("int")) \
            .withColumn("is_clothing",      (F.col("category") == "Clothing").cast("int")) \
            .withColumn("is_entertainment", (F.col("category") == "Entertainment").cast("int"))

        # =====================================================================
        # STEP 3: WINDOW FUNCTIONS
        # Per-user, time-ordered window features capture sequential behaviour:
        #   - amount_delta: change from the previous transaction (lag)
        #   - txn_rank:     position of each transaction in the user's history
        #   - running_total: cumulative spend up to this transaction
        # =====================================================================
        user_time_window = Window.partitionBy("user_id").orderBy("timestamp")

        df = df \
            .withColumn("prev_amount",    F.lag("amount", 1).over(user_time_window)) \
            .withColumn("amount_delta",   F.col("amount") - F.coalesce(F.col("prev_amount"), F.lit(0.0))) \
            .withColumn("txn_rank",       F.row_number().over(user_time_window)) \
            .withColumn("running_total",  F.sum("amount").over(
                user_time_window.rowsBetween(Window.unboundedPreceding, 0)))

        # =====================================================================
        # STEP 4: GROUP-LEVEL AGGREGATIONS
        # Collapse per-row features into per-user summary statistics.
        # =====================================================================
        df_features = df.groupBy("user_id").agg(
            # --- Amount statistics ---
            F.sum("amount").alias("total_spent"),
            F.mean("amount").alias("avg_transaction"),
            F.count("amount").alias("transaction_count"),
            F.max("amount").alias("max_transaction"),
            F.min("amount").alias("min_transaction"),
            F.stddev("amount").alias("stddev_transaction"),
            F.mean(F.log1p(F.col("amount"))).alias("avg_log_amount"),

            # --- Temporal features ---
            F.mean("hour_of_day").alias("avg_hour"),
            F.mean("day_of_week").alias("avg_day_of_week"),
            F.mean("is_weekend").alias("weekend_ratio"),
            F.countDistinct("month").alias("active_months"),

            # --- Categorical features ---
            F.sum("is_electronics").alias("electronics_count"),
            F.sum("is_groceries").alias("groceries_count"),
            F.sum("is_clothing").alias("clothing_count"),
            F.sum("is_entertainment").alias("entertainment_count"),
            F.mean("category_index").alias("avg_category_index"),

            # --- Window-derived features ---
            F.mean("amount_delta").alias("avg_amount_delta"),
            F.max("txn_rank").alias("total_transactions_ranked"),
            F.last("running_total").alias("final_running_total"),
        )

        # =====================================================================
        # STEP 5: NORMALIZATION (Z-SCORE)
        # Standardise key numeric features so they are mean-0 / std-1.
        # We collect global stats once then apply as a broadcast scalar to avoid
        # a second full shuffle.
        # =====================================================================
        stats = df_features.select(
            F.mean("total_spent").alias("mu_total"),
            F.stddev("total_spent").alias("sigma_total"),
            F.mean("avg_transaction").alias("mu_avg"),
            F.stddev("avg_transaction").alias("sigma_avg"),
            F.mean("transaction_count").alias("mu_count"),
            F.stddev("transaction_count").alias("sigma_count"),
        ).collect()[0]

        df_features = df_features \
            .withColumn("total_spent_zscore",
                        (F.col("total_spent") - stats["mu_total"]) / stats["sigma_total"]) \
            .withColumn("avg_transaction_zscore",
                        (F.col("avg_transaction") - stats["mu_avg"]) / stats["sigma_avg"]) \
            .withColumn("txn_count_zscore",
                        (F.col("transaction_count") - stats["mu_count"]) / stats["sigma_count"])

        # Deterministic output ordering
        df_features = df_features.orderBy("user_id")

        df_features.write.mode("overwrite").parquet(output_path)

        end_time = time.perf_counter()
        elapsed = end_time - start_time
        num_features = len(df_features.columns) - 1  # exclude user_id

        mlflow.log_metric("total_runtime_seconds", elapsed)
        mlflow.log_metric("num_features_engineered", num_features)

        print(f"Pipeline finished successfully!")
        print(f"Total Runtime : {elapsed:.2f} seconds")
        print(f"Features built: {num_features}")

        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run PySpark Feature Engineering Pipeline")
    parser.add_argument("--input",      type=str, required=True, help="Input data directory (Parquet)")
    parser.add_argument("--output",     type=str, required=True, help="Output data directory (Parquet)")
    parser.add_argument("--master",     type=str, default="local[*]",
                        help="Spark master URL (local[1] for baseline, local[*] for distributed)")
    parser.add_argument("--partitions", type=int, default=200,
                        help="Number of shuffle partitions")

    args = parser.parse_args()
    run_pipeline(args.input, args.output, args.master, args.partitions)
