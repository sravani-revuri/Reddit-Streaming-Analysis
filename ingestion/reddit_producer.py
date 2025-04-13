import praw
import json
from kafka import KafkaProducer
import time

# Setup Reddit API
reddit = praw.Reddit(
    client_id='WNlvBYoiez6dSy1U7L2RJA',
    client_secret='npfshKDZIhPYnvkGZ3xgECxCTog60Q',
    user_agent='python:reddit_streamer:v1.0 (by /u/sravz_24)'
)

# Setup Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Test if Reddit is accessible
try:
    subreddit = reddit.subreddit('PESU')
    print("Connected to Reddit and accessing the subreddit.")
except Exception as e:
    print(f"Error connecting to Reddit: {e}")
    exit(1)  # Exit the script if Reddit connection fails

# Fetch existing posts (latest 100 posts)
for post in subreddit.new(limit=5):  # Change 100 to any number based on how many posts you want to fetch
    data = {
        'id': post.id,
        'title': post.title,
        'score': post.score,
        'created_utc': post.created_utc,
        'num_comments': post.num_comments,
        'selftext': post.selftext,
    }
    print(f"Produced (existing post): {data}")  # Debug line to check
    producer.send('reddit-posts', value=data)

# Streaming new posts continuously
for post in subreddit.stream.submissions(skip_existing=True):  # Now, we can safely use skip_existing=True for continuous stream
    data = {
        'id': post.id,
        'title': post.title,
        'score': post.score,
        'created_utc': post.created_utc,
        'num_comments': post.num_comments,
        'selftext': post.selftext,
    }
    print(f"Produced (new post): {data}")  # Debug line to check
    producer.send('reddit-posts', value=data)
    time.sleep(1)  # Add a delay to avoid making too many requests to Reddit too quickly
