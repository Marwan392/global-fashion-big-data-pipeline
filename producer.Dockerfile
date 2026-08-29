FROM python:3.11-slim

WORKDIR /app

COPY kafka/producer.py /app/producer.py

RUN pip install --no-cache-dir kafka-python

CMD ["python", "/app/producer.py"]