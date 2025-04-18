from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, window, sum as _sum
from pyspark.sql.types import TimestampType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SentimentBatchAggregator") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Load from existing table (which has already been written by the stream processor)
sentiment_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/reddit_stream_db") \
    .option("dbtable", "sentiment_results") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Ensure the created_utc column is in the correct timestamp type
sentiment_df = sentiment_df.withColumn("created_utc", col("created_utc").cast(TimestampType()))

# Perform windowed aggregation over 3-minute windows
aggregated_df = sentiment_df.groupBy(
    window(col("created_utc"), "1 day")  # Using 3-minute window for aggregation
).agg(
    _sum(when(col("sentiment") == "positive", 1).otherwise(0)).alias("positive_count"),  # Count positive sentiment
    _sum(when(col("sentiment") == "negative", 1).otherwise(0)).alias("negative_count"),  # Count negative sentiment
    _sum(when(col("sentiment") == "neutral", 1).otherwise(0)).alias("neutral_count")  # Count neutral sentiment
)

# Select the window start and end times along with sentiment counts
final_df = aggregated_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    "positive_count",
    "negative_count",
    "neutral_count"
)

# Write the aggregated data to a new table 'sentiment_aggregated_batch'
final_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/reddit_stream_db") \
    .option("dbtable", "sentiment_aggregated_batch") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

print("✅ Aggregated sentiment written to 'sentiment_aggregated_batch'")
