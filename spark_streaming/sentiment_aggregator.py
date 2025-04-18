from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, from_json, current_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SentimentWindowAggregator") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema to match Kafka message format
schema = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("selftext", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("created_utc", DoubleType(), True),  # Not used here
    StructField("num_comments", IntegerType(), True),
    StructField("sentiment", StringType(), True)
])

# Read from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sentiment_analyse") \
    .option("startingOffsets", "latest") \
    .load()

# Parse Kafka JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Add processing time column
df_with_time = json_df.withColumn("processing_time", current_timestamp())

# Group by processing time window and sentiment and count the sentiment types
aggregated_df = df_with_time.groupBy(
    F.window(col("processing_time"), "30 seconds")  # Window size
).agg(
    # Count positive sentiment
    F.sum(F.when(col("sentiment") == "positive", 1).otherwise(0)).alias("positive_count"),
    # Count negative sentiment
    F.sum(F.when(col("sentiment") == "negative", 1).otherwise(0)).alias("negative_count"),
    # Count neutral sentiment
    F.sum(F.when(col("sentiment") == "neutral", 1).otherwise(0)).alias("neutral_count")
)

# Select and rename final output columns
aggregated_df = aggregated_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("positive_count"),
    col("negative_count"),
    col("neutral_count")
)

# Write each batch to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    print(f"Batch {batch_id} has {batch_df.count()} rows")
    batch_df.show(truncate=False)

    if batch_df.count() > 0:
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/reddit_stream_db") \
            .option("dbtable", "sentiment_aggregated") \
            .option("user", "root") \
            .option("password", "root") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

# Start the stream with batch processing
query = aggregated_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/spark_checkpoint") \
    .start()

query.awaitTermination()
