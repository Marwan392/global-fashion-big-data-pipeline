import csv
import json
import time
from kafka import KafkaProducer


KAFKA_SERVER = "kafka:9092"

FILES = {
    "customers": "/data/customers.csv",
    "products": "/data/products.csv",
    "stores": "/data/stores.csv",
    "employees": "/data/employees.csv",
    "discounts": "/data/discounts.csv",
    "transactions": "/data/transactions.csv"
}


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda value: json.dumps(
            value,
            ensure_ascii=False
        ).encode("utf-8")
    )


def send_csv(producer, topic, file_path):
    print(f"\nStarting: {file_path}")
    print(f"Kafka topic: {topic}")

    count = 0

    with open(
        file_path,
        "r",
        encoding="utf-8-sig",
        errors="replace",
        newline=""
    ) as file:

        reader = csv.DictReader(file)

        for row in reader:
            producer.send(topic, value=row)

            count += 1

            if count % 10000 == 0:
                producer.flush()
                print(f"{topic}: {count:,} records sent")

    producer.flush()

    print(f"Finished {topic}: {count:,} records")


def main():
    print("Connecting to Kafka...")

    producer = create_producer()

    print("Connected successfully.")

    for topic, file_path in FILES.items():
        send_csv(producer, topic, file_path)

    producer.close()

    print("\nAll CSV files have been sent to Kafka.")


if __name__ == "__main__":
    main()