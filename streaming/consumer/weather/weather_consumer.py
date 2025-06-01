from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'weather-api-topic',
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='weather-consumer-group'
)

print("Weather Consumer is running...")

for message in consumer:
    print(f"Received weather data: {message.value}")
