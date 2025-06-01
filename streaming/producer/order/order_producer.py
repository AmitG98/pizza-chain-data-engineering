from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime, timedelta

def generate_order():
    now = datetime.utcnow()
    delay = random.randint(-30, 30)
    return {
        "order_id": random.randint(100000, 999999),
        "timestamp": now.isoformat(),
        "customer_id": random.randint(2000, 2100),
        "store_id": random.choice([101, 102, 103]),
        "delivery_address": random.choice([
            "123 Main St, Tel Aviv",
            "45 Herzl St, Haifa",
            "88 Ben Yehuda St, Jerusalem"
        ]),
        "estimated_delivery_time": (now + timedelta(minutes=30)).isoformat(),
        "actual_delivery_time": (now + timedelta(minutes=30 + delay)).isoformat(),
        "status": random.choice(["delivered", "delayed", "canceled"])
    }

print("Waiting for Kafka to be ready...")
time.sleep(15)

print("Connecting to Kafka broker at kafka:9092")
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',  # בתוך דוקר, זה השם של הקונטיינר
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5
)

print("Producer is running and sending orders...")

while True:
    order = generate_order()
    print(f"Sending order: {order}")
    try:
        producer.send('orders-topic', order).get(timeout=10)  # שימוש ב־get כדי לוודא שליחה
    except Exception as e:
        print(f"Failed to send: {e}")
    time.sleep(3)
