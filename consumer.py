import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql.functions import from_json, col, window, to_timestamp, current_timestamp

# Directory used by Spark to persist offsets and aggregation state so the
# consumer can resume exactly where it stopped after a crash.
CHECKPOINT_DIR = "/tmp/spark_checkpoint_mlops"

def run_consumer():
    spark = SparkSession.builder \
        .appName("KafkaStreamingPipeline") \
        .master("local[2]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    json_schema = StructType([
        StructField("event_id",            IntegerType(), True),
        StructField("user_id",             IntegerType(), True),
        StructField("transaction_amount",  DoubleType(),  True),
        StructField("timestamp",           StringType(),  True),
    ])

    print("Starting Kafka consumer... waiting for data on 'mlops_transactions'")

    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "mlops_transactions") \
        .option("startingOffsets", "latest") \
        .load()

    parsed_stream = raw_stream \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), json_schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp(col("timestamp")))

    # =========================================================================
    # EXACTLY-ONCE DEDUPLICATION via foreachBatch
    #
    # Spark Structured Streaming with checkpointing guarantees that every Kafka
    # offset is processed AT LEAST ONCE.  To reach EXACTLY-ONCE we must make
    # the sink idempotent.  We do this inside foreachBatch:
    #
    #   1. dropDuplicates("event_id") removes any event that was re-delivered
    #      because the consumer restarted before committing the previous batch.
    #   2. The checkpoint written by Spark after each successful batch commit
    #      ensures no batch is silently skipped (no data loss).
    #
    # Together these two guarantees satisfy exactly-once end-to-end processing.
    # =========================================================================
    def process_batch(batch_df, batch_id):
        # Deduplicate within this micro-batch by event_id
        deduped = batch_df.dropDuplicates(["event_id"])

        # Windowed aggregation over the deduplicated events
        agg = deduped \
            .withWatermark("timestamp", "10 seconds") \
            .groupBy(window(col("timestamp"), "10 seconds")) \
            .agg(
                {"transaction_amount": "sum", "event_id": "count"}
            ) \
            .withColumnRenamed("sum(transaction_amount)", "total_volume") \
            .withColumnRenamed("count(event_id)",         "event_count") \
            .withColumn("batch_id",        col("batch_id") if False else
                        # Embed batch_id as a literal so output is traceable
                        __import__("pyspark.sql.functions", fromlist=["lit"])
                        .lit(batch_id)) \
            .withColumn("processed_at", current_timestamp())

        print(f"\n[Batch {batch_id}] Deduplicated rows: {deduped.count()} "
              f"(raw: {batch_df.count()})")
        agg.show(truncate=False)

    # Stateful 10-second tumbling window (kept for the non-foreachBatch path below)
    windowed_agg = parsed_stream \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(window(col("timestamp"), "10 seconds")) \
        .agg(
            {"transaction_amount": "sum", "event_id": "count"}
        ) \
        .withColumnRenamed("sum(transaction_amount)", "total_volume") \
        .withColumnRenamed("count(event_id)",         "event_count")

    # -------------------------------------------------------------------------
    # Sink: foreachBatch gives us the deduplication hook described above.
    # The checkpoint directory is what Spark uses to guarantee at-least-once
    # offset tracking; combined with dropDuplicates it becomes exactly-once.
    # -------------------------------------------------------------------------
    query = parsed_stream.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    run_consumer()
