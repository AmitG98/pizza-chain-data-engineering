from kafka import KafkaConsumer
import json
import time

print("Waiting for Kafka to be ready...")
time.sleep(20)

print("Trying to connect to Kafka...")
print("Creating KafkaConsumer...")
consumer = KafkaConsumer(
    'orders-topic',
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='debug-consumer-group'
)

print("Order Consumer is running and waiting for messages...")

for message in consumer:
    print(f"Received order: {message.value}")
