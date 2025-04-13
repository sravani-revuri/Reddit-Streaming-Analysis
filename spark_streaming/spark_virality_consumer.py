from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import *

spark = SparkSession.builder.appName("RedditVirality").getOrCreate()
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

# Simple virality score calculation (not ML)
def predict_virality(score, comments, text):
    length = len(text) if text else 0
    return float(0.5 * score + 0.3 * comments + 0.2 * length / 100)

virality_udf = udf(predict_virality, FloatType())
with_virality = json_df.withColumn("predicted_score", virality_udf(col("score"), col("num_comments"), col("title")))

def write_to_postgres(df, epoch_id):
    df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/reddit_stream_db") \
        .option("dbtable", "virality_scores") \
        .option("user", "root") \
        .option("password", "root") \
        .mode("append") \
        .save()

query = with_virality.writeStream.foreachBatch(write_to_postgres).start()
query.awaitTermination()
