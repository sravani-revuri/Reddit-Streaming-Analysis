from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# 1. Initialize Spark session
spark = SparkSession.builder \
    .appName("PostgresRedditTitleSentiment") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Set up VADER sentiment analyzer
analyzer = SentimentIntensityAnalyzer()

def get_sentiment(text):
    if not text:
        return "neutral"
    scores = analyzer.polarity_scores(text)
    compound = scores.get("compound", 0.0)
    if compound >= 0.05:
        return "positive"
    elif compound <= -0.05:
        return "negative"
    else:
        return "neutral"

# Register UDF
sentiment_udf = udf(get_sentiment, StringType())

# 3. Define JDBC configuration
jdbc_url = "jdbc:postgresql://localhost:5432/reddit_stream_db"
jdbc_opts = {
    "user": "root",
    "password": "root",
    "driver": "org.postgresql.Driver"
}

# 4. Read from `reddit_keyword_filtered` table
reddit_df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "reddit_keyword_filtered") \
    .options(**jdbc_opts) \
    .load()

# 5. Apply sentiment analysis on the 'title' column
df_with_sentiment = reddit_df.withColumn("title_sentiment", sentiment_udf(col("title")))

# 6. Write to `batch_title_sentiment` table
df_with_sentiment.select("id", "title", "title_sentiment") \
    .write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "batch_title_sentiment") \
    .options(**jdbc_opts) \
    .mode("overwrite") \
    .save()

print("✅ Batch sentiment analysis complete! Written to 'batch_title_sentiment'.")
