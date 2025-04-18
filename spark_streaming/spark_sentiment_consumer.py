from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, when
from pyspark.sql.types import *
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import to_json, struct

# Broadcast the analyzer to avoid serialization issues
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

# Define Kafka message schema to match the producer's data structure
schema = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("selftext", StringType(), True),  # Allow nullable values for selftext
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

# Parse the Kafka message and apply schema
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Handle missing selftext: If selftext is missing or empty, replace with a default string ("No content")
json_df = json_df.withColumn(
    "selftext", 
    when(col("selftext").isNull() | (col("selftext") == ""), "No content").otherwise(col("selftext"))
)

# Apply sentiment analysis
with_sentiment = json_df.withColumn("sentiment", sentiment_udf(col("title")))
# Convert epoch seconds (DoubleType) to TimestampType

with_sentiment = with_sentiment.withColumn("created_utc", from_unixtime(col("created_utc")).cast(TimestampType()))


def write_to_postgres(df, epoch_id):
    # Write to PostgreSQL
    df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/reddit_stream_db") \
        .option("dbtable", "sentiment_results") \
        .option("user", "root") \
        .option("password", "root") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    # Select fields you want to send to Kafka (convert to JSON string)
    kafka_df = df.select(
        to_json(struct(
            col("id"),
            col("title"),
            col("selftext"),
            col("score"),
            col("created_utc"),
            col("num_comments"),
            col("sentiment")
        )).alias("value")  # Kafka requires a "value" column
    )

    # Send to Kafka topic "sentiment_analyse"
    kafka_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "sentiment_analyse") \
        .save()

# Start the stream
query = with_sentiment.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()
