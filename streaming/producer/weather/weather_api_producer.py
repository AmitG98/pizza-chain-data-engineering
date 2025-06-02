# streaming/producer/weather_api_producer.py
from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

locations = ["Tel Aviv", "Haifa", "Jerusalem", "Eilat", "Beer Sheva"]
conditions = ["Clear", "Partly Cloudy", "Cloudy", "Rain", "Storm", "Fog"]

while True:
    weather = {
        "location": random.choice(locations),
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "temperature": round(random.uniform(10.0, 40.0), 1),
        "precipitation": round(random.uniform(0.0, 20.0), 1),
        "wind_speed": round(random.uniform(0.0, 15.0), 1),
        "weather_condition": random.choice(conditions)
    }

    print("Sending weather:", weather)
    producer.send("weather-api-topic", weather)
    time.sleep(3)  # כל 3 שניות תחזית חדשה