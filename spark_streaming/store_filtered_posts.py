from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime
from pyspark.sql.types import *

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Reddit Keyword Filtered Consumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Kafka and Postgres config
kafka_bootstrap_servers = "localhost:9092"
postgres_url = "jdbc:postgresql://localhost:5432/reddit_stream_db"
postgres_properties = {
    "user": "root",
    "password": "root",
    "driver": "org.postgresql.Driver"
}

# Define schema for Kafka data
schema = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("selftext", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("created_utc", DoubleType(), True),
    StructField("num_comments", IntegerType(), True)
])

# Read Kafka stream from the 'reddit-keyword-filtered' topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "reddit-keyword-filtered") \
    .option("startingOffsets", "latest") \
    .load()

# Parse and convert to structured DataFrame
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Convert created_utc (epoch) to TimestampType
json_df = json_df.withColumn("created_utc", from_unixtime(col("created_utc")).cast(TimestampType()))

# Function to write batch to PostgreSQL 
def write_to_postgres(batch_df, batch_id):
    batch_df.write.jdbc(
        url=postgres_url,
        table="reddit_raw_posts", 
        mode="append",
        properties=postgres_properties
    )

# Write stream to PostgreSQL
query = json_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()
