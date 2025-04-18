from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, when
from pyspark.sql.types import *
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pyspark.sql.functions import from_unixtime

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

# Parse and clean JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

json_df = json_df.withColumn(
    "selftext", 
    when(col("selftext").isNull() | (col("selftext") == ""), "No content").otherwise(col("selftext"))
)

# Convert created_utc to timestamp
json_df = json_df.withColumn("created_utc", from_unixtime(col("created_utc")).cast(TimestampType()))

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

# Start writing keyword-filtered data to PostgreSQL
raw_query = json_df.writeStream \
    .foreachBatch(write_filtered_data) \
    .outputMode("append") \
    .start()

# Apply sentiment analysis
with_sentiment = json_df.withColumn("sentiment", sentiment_udf(col("title")))

# Write sentiment results to PostgreSQL
def write_sentiment_data(df, epoch_id):
    df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/reddit_stream_db") \
        .option("dbtable", "sentiment_results") \
        .option("user", "root") \
        .option("password", "root") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# Start writing sentiment analysis results
sentiment_query = with_sentiment.writeStream \
    .foreachBatch(write_sentiment_data) \
    .outputMode("append") \
    .start()

# Await both streams
raw_query.awaitTermination()
sentiment_query.awaitTermination()
