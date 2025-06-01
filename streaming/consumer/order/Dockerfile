# consumer/order/Dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY order_consumer.py .

RUN pip install kafka-python

CMD ["python", "order_consumer.py"]
