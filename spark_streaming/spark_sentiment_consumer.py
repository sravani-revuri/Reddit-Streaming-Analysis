from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, when, from_unixtime, to_json, struct, current_timestamp
from pyspark.sql.types import *
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pyspark.sql.types import TimestampType

# Initialize Sentiment Analyzer
analyzer = SentimentIntensityAnalyzer()

# Define UDF for sentiment analysis
def get_sentiment(text):
    if not text:
        return "neutral"
    score = analyzer.polarity_scores(text)
    compound = score.get('compound', 0)
    if compound >= 0.05:
        return "positive"
    elif compound <= -0.05:
        return "negative"
    else:
        return "neutral"

# Register UDF
sentiment_udf = udf(get_sentiment, StringType())

# Initialize Spark session
spark = SparkSession.builder \
    .appName("RedditSentimentAnalysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define Kafka message schema
schema = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("selftext", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("created_utc", DoubleType(), True),
    StructField("num_comments", IntegerType(), True)
])

# Read from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reddit-keyword-filtered") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Handle missing selftext
json_df = json_df.withColumn(
    "selftext", 
    when(col("selftext").isNull() | (col("selftext") == ""), "No content").otherwise(col("selftext"))
)

# Convert created_utc (epoch seconds) to TimestampType (once!)
json_df = json_df.withColumn("created_utc", from_unixtime(col("created_utc")).cast(TimestampType()))

# Add ingestion time (capture the current time when the record is ingested)
json_df_time = json_df.withColumn("ingestion_time", current_timestamp())

# Write raw keyword-filtered data to another PostgreSQL table
def write_filtered_data(df, epoch_id):
    df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/reddit_stream_db") \
        .option("dbtable", "reddit_keyword_filtered") \
        .option("user", "root") \
        .option("password", "root") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# Start writing raw data to PostgreSQL
raw_query = json_df.writeStream \
    .foreachBatch(write_filtered_data) \
    .outputMode("append") \
    .start()


# Apply sentiment analysis
with_sentiment = json_df_time.withColumn("sentiment", sentiment_udf(col("title")))

# Write to PostgreSQL and send to Kafka
def write_to_postgres(df, epoch_id):
    print(f"\n=== Writing batch {epoch_id} ===")
    # Now include ingestion_time instead of created_utc
    df.select("id", "ingestion_time", "title", "sentiment").show(truncate=False)

    # Write to PostgreSQL
    df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/reddit_stream_db") \
        .option("dbtable", "sentiment_results") \
        .option("user", "root") \
        .option("password", "root") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    # Convert to JSON and write to Kafka
    kafka_df = df.select(
        to_json(struct(
            col("id"),
            col("title"),
            col("selftext"),
            col("score"),
            col("ingestion_time"),  # Insert ingestion time here
            col("num_comments"),
            col("sentiment")
        )).alias("value")
    )

    kafka_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "sentiment_analyse") \
        .save()

# Start the main sentiment stream
query = with_sentiment.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

# Await both
raw_query.awaitTermination()
query.awaitTermination()
