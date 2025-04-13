from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import *
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Spark setup
spark = SparkSession.builder.appName("RedditSentimentAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Kafka schema
schema = StructType([
    StructField("id", StringType()),
    StructField("title", StringType()),
    StructField("selftext", StringType()),
    StructField("score", IntegerType()),
    StructField("created_utc", DoubleType()),
    StructField("num_comments", IntegerType())
])

# Read Kafka stream
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reddit-posts") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")).select("data.*")

# Sentiment Analysis UDF
analyzer = SentimentIntensityAnalyzer()
def get_sentiment(text):
    if not text:
        return "neutral"
    score = analyzer.polarity_scores(text)
    return "positive" if score['compound'] >= 0.05 else "negative" if score['compound'] <= -0.05 else "neutral"

sentiment_udf = udf(get_sentiment, StringType())
with_sentiment = json_df.withColumn("sentiment", sentiment_udf(col("title")))

# Write to PostgreSQL
def write_to_postgres(df, epoch_id):
    df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/reddit_stream_db") \
        .option("dbtable", "sentiment_results") \
        .option("user", "root") \
        .option("password", "root") \
        .mode("append") \
        .save()

query = with_sentiment.writeStream.foreachBatch(write_to_postgres).start()
query.awaitTermination()
