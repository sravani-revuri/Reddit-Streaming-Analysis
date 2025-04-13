from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, window, current_timestamp
from pyspark.sql.types import *
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

spark = SparkSession.builder.appName("RedditAggregator").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("id", StringType()),
    StructField("title", StringType()),
    StructField("selftext", StringType()),
    StructField("score", IntegerType()),
    StructField("created_utc", DoubleType()),
    StructField("num_comments", IntegerType())
])

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reddit-posts") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")).select("data.*")

# Convert timestamp
df_with_ts = json_df.withColumn("timestamp", current_timestamp())

# Sentiment UDF
analyzer = SentimentIntensityAnalyzer()
def get_sentiment(text):
    if not text:
        return "neutral"
    score = analyzer.polarity_scores(text)
    return "positive" if score['compound'] >= 0.05 else "negative" if score['compound'] <= -0.05 else "neutral"

sentiment_udf = udf(get_sentiment, StringType())
df_with_sentiment = df_with_ts.withColumn("sentiment", sentiment_udf(col("title")))

# Aggregation
aggregated = df_with_sentiment.groupBy(
    window(col("timestamp"), "15 minutes"),
    col("sentiment")
).count()

# Output to console (or you can write to DB)
query = aggregated.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()
