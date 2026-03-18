from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql.functions import from_json, col, window, to_timestamp

def run_consumer():
    # We must inject the Spark-Kafka package so PySpark knows how to read from Kafka
    spark = SparkSession.builder \
        .appName("KafkaStreamingPipeline") \
        .master("local[2]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Define schema matching our producer's JSON
    json_schema = StructType([
        StructField("event_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("transaction_amount", DoubleType(), True),
        StructField("timestamp", StringType(), True)
    ])

    print("Starting Kafka consumer... waiting for data on 'mlops_transactions'")

    # Read live stream from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "mlops_transactions") \
        .option("startingOffsets", "latest") \
        .load()

    # Kafka values are binary, so we cast to string and parse the JSON
    parsed_stream = raw_stream \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), json_schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp(col("timestamp")))

    # Stateful logic: 10-second tumbling window aggregation [cite: 39]
    windowed_agg = parsed_stream \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(window(col("timestamp"), "10 seconds")) \
        .agg(
            {"transaction_amount": "sum", "event_id": "count"}
        ) \
        .withColumnRenamed("sum(transaction_amount)", "total_volume") \
        .withColumnRenamed("count(event_id)", "event_count")

    # Sink the results directly to the console so we can see it working
    query = windowed_agg.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    run_consumer()