import json
import os
import requests

from kafka import KafkaConsumer
from kafka.structs import TopicPartition, OffsetAndMetadata


KAFKA_SERVER = "kafka:9092"

TOPICS = [
    "customers",
    "products",
    "stores",
    "employees",
    "discounts",
    "transactions"
]

GROUP_ID = "fashion-hdfs-consumer-v2"

BATCH_SIZE = 10000

HDFS_URL = "http://namenode:9870/webhdfs/v1"
HDFS_BASE = "/data/raw"

TEMP_DIR = "/tmp/kafka_batches"

# If Kafka has no new messages for this long,
# upload the remaining partial batches.
IDLE_TIMEOUT_MS = 10000


def create_dir(path):

    response = requests.put(
        f"{HDFS_URL}{path}",
        params={
            "op": "MKDIRS",
            "user.name": "root"
        },
        timeout=30
    )

    if response.status_code not in (200, 201):

        raise Exception(
            f"Cannot create {path}: "
            f"{response.status_code} {response.text}"
        )


def upload(local_file, hdfs_file):

    # Create a NEW HDFS file
    response = requests.put(
        f"{HDFS_URL}{hdfs_file}",
        params={
            "op": "CREATE",
            "overwrite": "false",
            "user.name": "root"
        },
        allow_redirects=False,
        timeout=30
    )

    if response.status_code != 307:

        raise Exception(
            f"CREATE failed: "
            f"{response.status_code} {response.text}"
        )

    upload_url = response.headers["Location"]

    with open(local_file, "rb") as f:

        response = requests.put(
            upload_url,
            data=f,
            timeout=600
        )

    if response.status_code not in (200, 201):

        raise Exception(
            f"Upload failed: "
            f"{response.status_code} {response.text}"
        )


def write_batch(topic, partition, batch):

    if not batch["records"]:
        return

    os.makedirs(
        os.path.join(TEMP_DIR, topic),
        exist_ok=True
    )

    filename = (
        f"part-{partition}-"
        f"{batch['part']:05d}.json"
    )

    local_file = os.path.join(
        TEMP_DIR,
        topic,
        filename
    )

    # Write JSON Lines
    with open(
        local_file,
        "w",
        encoding="utf-8"
    ) as f:

        for record in batch["records"]:

            f.write(
                json.dumps(
                    record,
                    ensure_ascii=False
                ) + "\n"
            )

    hdfs_file = (
        f"{HDFS_BASE}/"
        f"{topic}/"
        f"{filename}"
    )

    print(
        f"Uploading {topic}: "
        f"{len(batch['records']):,} records",
        flush=True
    )

    upload(
        local_file,
        hdfs_file
    )

    os.remove(local_file)

    # Commit only after successful HDFS upload
    tp = TopicPartition(
        topic,
        partition
    )

    consumer.commit({
        tp: OffsetAndMetadata(
            batch["last_offset"] + 1,
            ""
        )
    })

    print(
        f"✓ {hdfs_file}",
        flush=True
    )

    batch["records"] = []

    batch["part"] += 1


def main():

    global consumer

    os.makedirs(
        TEMP_DIR,
        exist_ok=True
    )

    consumer = KafkaConsumer(
        *TOPICS,

        bootstrap_servers=KAFKA_SERVER,

        group_id=GROUP_ID,

        auto_offset_reset="earliest",

        enable_auto_commit=False,

        max_poll_records=1000,

        value_deserializer=lambda x: json.loads(
            x.decode("utf-8")
        )
    )

    print(
        "Kafka → HDFS RAW ingestion started",
        flush=True
    )

    # Create topic directories
    for topic in TOPICS:

        create_dir(
            f"{HDFS_BASE}/{topic}"
        )

    batches = {}

    try:

        while True:

            records = consumer.poll(
                timeout_ms=IDLE_TIMEOUT_MS,
                max_records=1000
            )

            # No messages for 10 seconds
            if not records:

                print(
                    "No new Kafka messages. "
                    "Uploading remaining batches...",
                    flush=True
                )

                for (topic, partition), batch in list(
                    batches.items()
                ):

                    write_batch(
                        topic,
                        partition,
                        batch
                    )

                print(
                    "✓ HDFS RAW ingestion finished.",
                    flush=True
                )

                break

            for tp, messages in records.items():

                for message in messages:

                    key = (
                        message.topic,
                        message.partition
                    )

                    if key not in batches:

                        batches[key] = {
                            "records": [],
                            "last_offset": message.offset,
                            "part": 0
                        }

                    batch = batches[key]

                    batch["records"].append(
                        message.value
                    )

                    batch["last_offset"] = (
                        message.offset
                    )

                    if len(batch["records"]) >= BATCH_SIZE:

                        write_batch(
                            message.topic,
                            message.partition,
                            batch
                        )

    except KeyboardInterrupt:

        print(
            "Consumer stopped manually.",
            flush=True
        )

    finally:

        consumer.close()


if __name__ == "__main__":
    main()