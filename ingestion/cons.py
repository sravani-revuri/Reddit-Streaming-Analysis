from kafka import KafkaConsumer
import json

# Setup Kafka Consumer
consumer = KafkaConsumer(
    'reddit-posts',  # Kafka topic to consume
    bootstrap_servers='localhost:9092',  # Kafka broker
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize JSON data
)

# Consuming messages from Kafka
print("Consumer started, waiting for messages...")
for message in consumer:
    print(f"Received message: {message.value}")
