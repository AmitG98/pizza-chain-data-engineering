FROM python:3.9-slim

WORKDIR /app
COPY order_producer.py .

RUN pip install kafka-python

CMD ["python", "order_producer.py"]
