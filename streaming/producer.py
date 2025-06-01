from kafka import KafkaProducer
import json
import time
from faker import Faker
import random
from kafka.errors import NoBrokersAvailable

fake = Faker()

while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Connected to Kafka successfully.")
        break
    except NoBrokersAvailable:
        print("Kafka not ready, retrying in 5 seconds...")
        time.sleep(5)

topic = 'orders'

while True:
    order = {
        "order_id": fake.uuid4(),
        "customer_name": fake.name(),
        "amount": round(random.uniform(10.0, 200.0), 2),
        "timestamp": fake.iso8601()
    }
    producer.send(topic, order)
    print(f"Sent: {order}")
    time.sleep(2)
