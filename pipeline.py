import argparse
import time
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def run_pipeline(input_path: str, output_path: str, master: str, partitions: int):
    print(f"Starting PySpark pipeline on master: {master} with {partitions} partitions...")
    
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("DistributedFeatureEngineering") \
        .master(master) \
        .config("spark.sql.shuffle.partitions", partitions) \
        .getOrCreate()
        
    start_time = time.perf_counter()
    
    # Load Data
    df = spark.read.parquet(input_path)
    
    # Feature Engineering logic (simulating Module 5 requirements)
    df_features = df.filter(F.col("amount") > 0) \
        .withColumn("log_amount", F.log1p(F.col("amount"))) \
        .groupBy("user_id") \
        .agg(
            F.sum("amount").alias("total_spent"),
            F.mean("amount").alias("avg_transaction"),
            F.count("amount").alias("transaction_count")
        )
    
    # Write output
    df_features.write.mode("overwrite").parquet(output_path)
    
    end_time = time.perf_counter()
    elapsed = end_time - start_time
    
    print(f"Pipeline finished successfully!")
    print(f"Total Runtime: {elapsed:.2f} seconds")
    
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run PySpark Feature Engineering Pipeline")
    parser.add_argument("--input", type=str, required=True, help="Input data directory")
    parser.add_argument("--output", type=str, required=True, help="Output data directory")
    parser.add_argument("--master", type=str, default="local[*]", help="Spark master URL (local[1] for baseline, local[*] for distributed)")
    parser.add_argument("--partitions", type=int, default=200, help="Number of shuffle partitions")
    
    args = parser.parse_args()
    run_pipeline(args.input, args.output, args.master, args.partitions)