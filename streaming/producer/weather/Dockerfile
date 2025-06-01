FROM python:3.9-slim

WORKDIR /app
COPY weather_api_producer.py .

RUN pip install kafka-python
CMD ["python", "weather_api_producer.py"]
