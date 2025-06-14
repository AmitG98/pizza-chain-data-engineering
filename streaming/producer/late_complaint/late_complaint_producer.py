from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime, timedelta

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

order_ids = list(range(100000, 100500))
complaint_types = [
    ("Late delivery", "Order arrived more than 30 minutes late"),
    ("Wrong order", "Received a different pizza than ordered"),
    ("Cold food", "Pizza arrived cold"),
    ("Missing item", "Drinks or extras were missing"),
    ("Bad service", "Delivery person was rude or unhelpful")
]

while True:
    timestamp_received = datetime.utcnow() - timedelta(hours=random.randint(1, 48))
    complaint_type, description = random.choice(complaint_types)

    complaint = {
        "complaint_id": random.randint(10000, 99999),
        "order_id": random.choice(order_ids),
        "timestamp_received": timestamp_received.isoformat(),
        "complaint_type": complaint_type,
        "description": description
    }

    print("Sending complaint:", complaint)
    producer.send('complaints-late-topic', complaint)
    time.sleep(random.randint(3, 6))