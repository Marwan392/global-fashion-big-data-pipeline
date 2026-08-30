FROM python:3.11-slim

WORKDIR /app

COPY kafka/consumer.py /app/consumer.py

RUN pip install --no-cache-dir kafka-python requests

CMD ["python", "-u", "/app/consumer.py"]