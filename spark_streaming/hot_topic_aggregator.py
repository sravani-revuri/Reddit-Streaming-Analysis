from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import explode, split, col, lower, window
from pyspark.sql import DataFrame
from pyspark.ml.feature import StopWordsRemover
import pandas as pd
import logging
from sqlalchemy import create_engine

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("RedditTopTrendingWord") \
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("WARN")

# Kafka config
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'hot-topic'

# Load Kafka Stream as a DataFrame
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Parse Kafka JSON data
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)", "timestamp as kafka_timestamp") \
    .select(
        F.from_json("value", 
            "STRUCT<id: STRING, title: STRING, score: INT, created_utc: DOUBLE, num_comments: INT, selftext: STRING>"
        ).alias("data"),
        "kafka_timestamp"
    )

# Extract title
titles_df = parsed_df.select("data.title", "kafka_timestamp")

# Tokenize and lowercase the title into arrays of words
titles_array_df = titles_df.withColumn("words_array", split(lower(col("title")), r"\W+"))

# Remove stop words using StopWordsRemover
remover = StopWordsRemover(inputCol="words_array", outputCol="filtered_words")
filtered_df = remover.transform(titles_array_df)

# Explode words and filter out empty strings
cleaned_words_df = filtered_df.select(explode(col("filtered_words")).alias("word"), "kafka_timestamp")
cleaned_words_df = cleaned_words_df.filter(cleaned_words_df["word"] != "")

# Perform windowed word count (1-minute windows)
windowed_df = cleaned_words_df \
    .groupBy(window(col("kafka_timestamp"), "1 minute"), "word") \
    .count() \
    .withColumn("window_start", col("window.start")) \
    .withColumn("window_end", col("window.end")) \
    .select("window_start", "window_end", "word", "count")

# Function to write top trending word per window to PostgreSQL
def write_top_word_to_postgres(df: DataFrame, epoch_id: int):
    if df.isEmpty():
        return

    # Convert to Pandas
    pdf = df.toPandas()

    # Ensure datetime columns are actually datetime
    pdf['window_start'] = pd.to_datetime(pdf['window_start'])
    pdf['window_end'] = pd.to_datetime(pdf['window_end'])

    # Add batch ID to the dataframe (use epoch_id or any unique identifier for the batch)
    pdf['batch_id'] = epoch_id

    # Group by window and find max count
    top_words_per_window = pdf.groupby(['window_start', 'window_end'])['count'].transform(max)
    top_words_df = pdf[pdf['count'] == top_words_per_window]

    # Write to PostgreSQL
    engine = create_engine("postgresql://root:root@localhost:5432/reddit_stream_db")
    top_words_df.to_sql("trending_words_batch", engine, if_exists="append", index=False)

# Start the stream
query = windowed_df.writeStream \
    .foreachBatch(write_top_word_to_postgres) \
    .outputMode("complete") \
    .start()

query.awaitTermination()

# Stop Spark session on shutdown
spark.stop()
