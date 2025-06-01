# streaming/producer/order_producer.py
from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime, timedelta
import time

print("Waiting for Kafka to be ready...")
time.sleep(10)

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

store_ids = [101, 102, 103]
customer_ids = list(range(2000, 2100))
addresses = [
    "123 Main St, Tel Aviv",
    "45 Herzl St, Haifa",
    "88 Ben Yehuda St, Jerusalem"
]
statuses = ["delivered", "delayed", "canceled"]

while True:
    timestamp = datetime.utcnow()
    estimated_minutes = random.randint(20, 60)
    offset = random.randint(-10, 30)
    actual_minutes = estimated_minutes + offset

    order = {
        "order_id": random.randint(100000, 999999),
        "timestamp": timestamp.isoformat(),
        "customer_id": random.choice(customer_ids),
        "store_id": random.choice(store_ids),
        "delivery_address": random.choice(addresses),
        "estimated_delivery_time": (timestamp + timedelta(minutes=estimated_minutes)).isoformat(),
        "actual_delivery_time": (timestamp + timedelta(minutes=actual_minutes)).isoformat(),
        "status": random.choices(
            statuses, weights=[0.7, 0.2, 0.1], k=1
        )[0]  # סיכוי גבוה להצלחה
    }

    print("Sending order:", order)
    producer.send('orders-topic', order)
    time.sleep(random.randint(2, 5))