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
    subreddit = reddit.subreddit('Naruto')
    print("Connected to Reddit and accessing the subreddit.")
except Exception as e:
    print(f"Error connecting to Reddit: {e}")
    exit(1)

# Function to fetch posts one-by-one
def fetch_posts():
    # You can adjust the limit or use 'new()' for most recent posts, 'hot()' for hot posts, etc.
    for post in subreddit.hot(limit=None):  # Fetch posts sequentially
        yield post  # This will yield each post one by one

# Continuously fetch and send posts to Kafka
try:
    # Infinite loop to keep fetching posts
    for post in fetch_posts():
        # Prepare the data for Kafka
        data = {
            'id': post.id,
            'title': post.title,
            'score': post.score,
            'created_utc': post.created_utc,
            'num_comments': post.num_comments,
            'selftext': post.selftext if post.selftext else ""  # Check if selftext exists, else use empty string
        }
        print(f"Produced (existing post): {data}")
        producer.send('hot-topic', value=data)
        
        # Sleep for a moment to control the rate of fetching (adjust as necessary)
        time.sleep(1)

except KeyboardInterrupt:
    print("Stream interrupted by user.")
finally:
    # Close the producer gracefully
    producer.flush()
    producer.close()
    print("Producer closed.")
