from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime, timedelta

EVENT_TYPES = [
    "order_received",
    "order_in_preparation",
    "order_sent_out_for_delivery",
    "order_delivered"
]

def generate_order_base():
    delay_minutes = random.randint(0, 72 * 60)
    event_time = datetime.utcnow() - timedelta(minutes=delay_minutes)

    return {
        "order_id": random.randint(100000, 999999),
        "event_time": event_time.isoformat(),
        "timestamp": datetime.utcnow().isoformat(),
        "customer_id": random.randint(2000, 2100),
        "store_id": random.choice([101, 102, 103]),
        "delivery_address": random.choice([
            "123 Main St, Tel Aviv",
            "45 Herzl St, Haifa",
            "88 Ben Yehuda St, Jerusalem"
        ]),
        "estimated_delivery_time": (event_time + timedelta(minutes=30)).isoformat(),
    }

def generate_order_events(order):
    order_time = datetime.fromisoformat(order["event_time"])

    # שונות בין הזמנות
    prep_durations = {
        "simple": random.randint(5, 10),
        "medium": random.randint(10, 20),
        "complex": random.randint(20, 30)
    }

    delivery_durations = {
        "nearby": random.randint(10, 20),
        "medium_distance": random.randint(20, 30),
        "far": random.randint(30, 45)
    }

    order_type = random.choice(list(prep_durations.keys()))
    distance = random.choice(list(delivery_durations.keys()))
    random_delay = random.randint(-5, 10)

    prep_time = order_time + timedelta(minutes=prep_durations[order_type])
    out_for_delivery_time = prep_time + timedelta(minutes=delivery_durations[distance])

    # לוודא שזמן המסירה אחרי זמן היציאה
    delivered_time = out_for_delivery_time + timedelta(minutes=max(5, random_delay))

    events = [
        {
            "order_id": order["order_id"],
            "customer_id": order["customer_id"],
            "store_id": order["store_id"],
            "event_time": order_time.isoformat(),
            "timestamp": datetime.utcnow().isoformat(),
            "event_type": "order_received"
        },
        {
            "order_id": order["order_id"],
            "customer_id": order["customer_id"],
            "store_id": order["store_id"],
            "event_time": prep_time.isoformat(),
            "timestamp": datetime.utcnow().isoformat(),
            "event_type": "order_in_preparation"
        },
        {
            "order_id": order["order_id"],
            "customer_id": order["customer_id"],
            "store_id": order["store_id"],
            "event_time": out_for_delivery_time.isoformat(),
            "timestamp": datetime.utcnow().isoformat(),
            "event_type": "order_sent_out_for_delivery"
        },
        {
            "order_id": order["order_id"],
            "customer_id": order["customer_id"],
            "store_id": order["store_id"],
            "event_time": delivered_time.isoformat(),
            "timestamp": datetime.utcnow().isoformat(),
            "event_type": "order_delivered"
        }
    ]

    return events, delivered_time

print("Waiting for Kafka to be ready...")
time.sleep(15)

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5
)

print("Producer is running and sending orders & events...")

while True:
    order = generate_order_base()

    # יצירת סדרת האיוונטים + חישוב זמן המסירה בפועל
    events, delivered_time = generate_order_events(order)

    # נעדכן את ההזמנה עם זמן המסירה האמיתי
    order["actual_delivery_time"] = delivered_time.isoformat()

    print(f"Sending order: {order}")
    try:
        producer.send('orders-topic', order).get(timeout=10)

        for event in events:
            print(f"Sending event: {event}")
            producer.send('orders-events-topic', event).get(timeout=10)
            time.sleep(1)
    except Exception as e:
        print(f"Failed to send: {e}")
    time.sleep(3)
