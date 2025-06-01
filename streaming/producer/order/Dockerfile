FROM python:3.9-slim

WORKDIR /app
COPY . .

RUN pip install kafka-python

CMD ["python", "order_producer.py"]
