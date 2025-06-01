FROM python:3.9-slim

WORKDIR /app
COPY late_complaint_producer.py .

RUN pip install kafka-python

CMD ["python", "late_complaint_producer.py"]
