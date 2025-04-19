from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SentimentWindowAggregator") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema matching Kafka value JSON (use ingestion_time instead of created_utc)
schema = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("selftext", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("ingestion_time", TimestampType(), True),  # Use ingestion_time from Kafka
    StructField("num_comments", IntegerType(), True),
    StructField("sentiment", StringType(), True)
])

# Read Kafka stream
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sentiment_analyse") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON payload
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Group by 2-hour window based on ingestion_time (not processing time)
aggregated_df = json_df.groupBy(
    window(col("ingestion_time"), "1 minute")  # Window based on ingestion_time
).agg(
    F.sum(F.when(col("sentiment") == "positive", 1).otherwise(0)).alias("positive_count"),
    F.sum(F.when(col("sentiment") == "negative", 1).otherwise(0)).alias("negative_count"),
    F.sum(F.when(col("sentiment") == "neutral", 1).otherwise(0)).alias("neutral_count")
)

# Select output format
aggregated_df = aggregated_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    "positive_count",
    "negative_count",
    "neutral_count"
)

# Define write to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    print(f"\n===== Batch {batch_id} =====")
    batch_df.show(truncate=False)  # 👈 This prints the data to console

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

# Start streaming query
query = aggregated_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("update") \
    .start()

query.awaitTermination()
