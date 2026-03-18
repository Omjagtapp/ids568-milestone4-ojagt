import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
TOPIC_NAME = "mlops_transactions"

def generate_event(event_id):
    return {
        "event_id": event_id,
        "user_id": random.randint(1, 1000),
        "transaction_amount": round(random.uniform(10.0, 500.0), 2),
        "timestamp": datetime.now().isoformat()
    }

print(f"Starting Kafka producer on topic '{TOPIC_NAME}'... Press Ctrl+C to stop.")
event_count = 0

try:
    while True:
        # Simulate bursty traffic: 1 to 50 events per batch
        batch_size = random.randint(1, 50)
        for i in range(batch_size):
            event = generate_event(event_count + i)
            producer.send(TOPIC_NAME, value=event)
        
        producer.flush()
        event_count += batch_size
        print(f"Produced {batch_size} events to Kafka (Total: {event_count})")
        
        # Sleep to simulate real-world streaming delays
        time.sleep(random.uniform(0.5, 2.0))
        
except KeyboardInterrupt:
    print("\nProducer stopped successfully.")
    producer.close()