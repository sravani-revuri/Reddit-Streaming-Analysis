from kafka import KafkaConsumer
import json
import signal
import sys

# Setup Kafka Consumer
consumer = KafkaConsumer(
    'reddit-raw-posts',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def shutdown_handler(sig, frame):
    print("\nShutting down consumer...")
    consumer.close()
    sys.exit(0)

# Register the signal handler for graceful shutdown
signal.signal(signal.SIGINT, shutdown_handler)

print("Consumer started, waiting for messages...\n")
try:
    for message in consumer:
        print("Received message:")
        for key, value in message.value.items():
            print(f"{key}: {value}")
        print("-" * 40)  # Separator for clarity
except KeyboardInterrupt:
    shutdown_handler(None, None)
