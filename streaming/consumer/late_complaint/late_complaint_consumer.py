from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'complaints-late-topic',
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='late-complaint-consumer-group'
)

print("Late Complaint Consumer is running...")

for message in consumer:
    print(f"Received late complaint: {message.value}")
