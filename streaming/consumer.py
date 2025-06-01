# consumer.py
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest',  
    enable_auto_commit=True,
    group_id='consumer-group-1',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("✅ Connected to Kafka — waiting for messages...\n")
for message in consumer:
    print(f"🟢 Received: {message.value}")
