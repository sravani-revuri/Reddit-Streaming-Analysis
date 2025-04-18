from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import *
from kafka import KafkaProducer
import json

# Initialize Spark session
spark = SparkSession.builder \
    .appName("RedditTitleFilter") \
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

# Read from Kafka (reddit-raw-posts topic)
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reddit-raw-posts") \
    .option("startingOffsets", "latest") \
    .load()

# Parse the Kafka message and apply schema
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Function to filter posts by the keyword 'Naruto' in the title
def filter_by_naruto(title):
    if title and "sasuke" in title.lower():  # Check if 'naruto' is in the title (case insensitive)
        return True
    return False

# Register the filter function as a UDF
filter_udf = udf(filter_by_naruto, BooleanType())

# Apply the filter UDF to filter posts by the keyword 'Naruto' in the title
filtered_df = json_df.filter(filter_udf(col("title")))

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to send filtered posts to Kafka
def send_to_kafka(df, epoch_id):
    # Convert the DataFrame to a Pandas DataFrame for easier handling in the loop
    pandas_df = df.toPandas()
    
    for index, row in pandas_df.iterrows():
        # Prepare data for sending to Kafka
        data = {
            'id': row['id'],
            'title': row['title'],
            'score': row['score'],
            'created_utc': row['created_utc'],
            'num_comments': row['num_comments'],
            'selftext': row['selftext'] if row['selftext'] else ""
        }
        
        print(f"Sending filtered post: {data}")
        producer.send('reddit-keyword-filtered', value=data)

# Write stream with the custom function
query = filtered_df.writeStream \
    .foreachBatch(send_to_kafka) \
    .outputMode("append") \
    .start()

query.awaitTermination()
