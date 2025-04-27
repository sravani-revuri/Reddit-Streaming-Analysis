import praw
import json
from kafka import KafkaProducer
import time

# Setup Reddit API
reddit = praw.Reddit(
    client_id='YOUR_CLIENT_ID',
    client_secret='YOUR_CLIENT_KEY',
    user_agent='AGENT'
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

# Function to fetch existing posts
def fetch_existing_posts():
    # You can adjust the limit or use the 'new()' function for most recent posts, 'hot()' for hot posts, etc.
    return subreddit.new(limit=None)  # Get all existing posts (it will keep fetching in chunks)

# Continuously fetch and send existing posts to Kafka
try:
    # Infinite loop to keep fetching posts
    for post in fetch_existing_posts():
        data = {
            'id': post.id,
            'title': post.title,
            'score': post.score,
            'created_utc': post.created_utc,
            'num_comments': post.num_comments,
            'selftext': post.selftext if post.selftext else ""  # Check if selftext exists, else use empty string
        }
        print(f"Produced (existing post): {data}")
        producer.send('reddit-raw-posts', value=data)
        
        # Sleep for a moment to control the rate of fetching (adjust as necessary)
        time.sleep(1)

except KeyboardInterrupt:
    print("Stream interrupted by user.")
finally:
    # Close the producer gracefully
    producer.flush()
    producer.close()
    print("Producer closed.")
