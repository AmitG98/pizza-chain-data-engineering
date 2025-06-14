from kafka import KafkaConsumer
import json
import time

print("Waiting for Kafka to be ready...")
time.sleep(20)

print("Connecting to Kafka...")
consumer = KafkaConsumer(
    'orders-topic', 'orders-events-topic',
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='debug-combined-consumer'
)

print("Combined Consumer is running...")

for message in consumer:
    msg = message.value
    if message.topic == 'orders-topic':
        print(f"[ORDER] Order ID: {msg['order_id']} | ETA: {msg['estimated_delivery_time']} | Actual: {msg.get('actual_delivery_time', 'N/A')}")
    else:
        print(f"[EVENT] Order ID: {msg['order_id']} | Type: {msg['event_type']} | Time: {msg['event_time']}")
