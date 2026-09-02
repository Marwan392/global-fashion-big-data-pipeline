# Fashion Retail Data Pipeline

An end-to-end big data pipeline for a fashion retail business. This is a **batch pipeline** — Kafka is used purely as a durable message queue to move CSV data one time from source to storage. Raw data is loaded through **Apache Kafka**, landed in **HDFS**, cleaned and modeled with **Apache Spark**, and the results are split across two reporting layers: SQL analytics feed a **Power BI** dashboard, while ML model outputs are served through a **Streamlit** dashboard backed by a **FastAPI** service.

## Table of Contents

- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
- [Services & Ports](#services--ports)
- [Data](#data)
- [Kafka Layer](#kafka-layer)
- [HDFS Layer](#hdfs-layer)
- [Spark Layer](#spark-layer)
- [API & Dashboard](#api--dashboard)
- [Environment Variables](#environment-variables)
- [Troubleshooting](#troubleshooting)
- [Power BI Dashboard](#power-bi-dashboard)
- [License](#license)

## Architecture

```
 ┌─────────────┐
 │  CSV Files  │  data/customers.csv, products.csv, stores.csv,
 │             │  employees.csv, discounts.csv, transactions.csv
 └──────┬──────┘
        │  read row-by-row
        ▼
 ┌─────────────────┐        publish JSON        ┌──────────────┐
 │ Kafka Producer   │ ───────────────────────►   │ Kafka Broker │
 │ (producer.py)    │        per-topic           │ (KRaft mode) │
 └──────────────────┘                            └──────┬───────┘
                                                          │ consume
                                                          ▼
                                                 ┌──────────────────┐
                                                 │ Kafka Consumer    │
                                                 │ (consumer.py)     │
                                                 │ batches + commits │
                                                 │ offsets on success│
                                                 └────────┬──────────┘
                                                          │ WebHDFS PUT
                                                          ▼
                                                 ┌──────────────────┐
                                                 │ HDFS Raw Zone     │
                                                 │ /data/raw/<topic> │
                                                 └────────┬──────────┘
                                                          │ read
                                                          ▼
                                                 ┌──────────────────────────┐
                                                 │ Spark Cluster             │
                                                 │ - cleaning.py             │
                                                 │ - transformation.py       │
                                                 │ - feature_engineering.py  │
                                                 │ - customer_segmentation.py│
                                                 │ - customer_churn_         │
                                                 │   prediction.py           │
                                                 └──────┬─────────────┬──────┘
                                                         │             │
                                          ML results     │             │  analytics.sql
                                                         ▼             ▼
                                                ┌──────────────┐  ┌──────────────┐
                                                │ FastAPI       │  │ Power BI     │
                                                │ Backend       │  │ Dashboard    │
                                                │ (backend.py)  │  └──────────────┘
                                                └──────┬────────┘
                                                        │ HTTP
                                                        ▼
                                                ┌──────────────────┐
                                                │ Streamlit App     │
                                                │ (app.py)          │
                                                │ model results     │
                                                └──────────────────┘
```

**Pipeline stages:**

1. **Batch ingestion** — `producer.py` reads each CSV file (`customers`, `products`, `stores`, `employees`, `discounts`, `transactions`) once, row by row, and publishes it as JSON to its corresponding Kafka topic, flushing every 10,000 records. This is a one-shot load — the producer exits once every file has been sent.
2. **Raw landing** — `consumer.py` subscribes to all six topics as a single consumer group, buffers records into batches of 10,000, and uploads each batch to HDFS via the WebHDFS REST API. Kafka offsets are only committed after a batch is successfully written to HDFS, so a crash mid-run cannot silently drop or duplicate data. The consumer automatically finishes once the broker has been idle for 10 seconds (i.e., the full batch load is done), flushing any partial batches first.
3. **Processing & modeling** — Spark jobs under `spark/` clean the raw JSON, engineer features, and run customer segmentation and churn prediction models. Separately, the SQL analytics defined in `spark/sql/analytics.sql` are run to feed reporting.
4. **Reporting split:**
   - `analytics.sql` results feed the **Power BI** dashboard directly.
   - ML model outputs (segmentation, churn) are served by `backend.py` (FastAPI/Uvicorn) and visualized in the **Streamlit** app (`app.py`).

## Tech Stack

| Layer         | Technology                                  |
|---------------|----------------------------------------------|
| Streaming     | Apache Kafka 4.1.0 (KRaft mode, no ZooKeeper) |
| Storage       | HDFS — `bde2020/hadoop` images (Hadoop 3.2.1 / Java 8) |
| Processing/ML | Apache Spark 3.5.6 (`apache/spark:3.5.6` image), standalone cluster (1 master + 1 worker) |
| API           | FastAPI, served with Uvicorn |
| Dashboard     | Streamlit (ML results), Power BI (SQL analytics) |
| Orchestration | Docker Compose |

## Project Structure

```
final_proj/
├── data/                              # Source CSV files (mounted read-only into containers)
│   ├── customers.csv
│   ├── discounts.csv
│   ├── employees.csv
│   ├── products.csv
│   ├── stores.csv
│   └── transactions.csv
├── hadoop-config/                     # Hadoop client config shared by consumer + Spark
├── kafka/
│   ├── producer.py                    # CSV → Kafka topic publisher
│   └── consumer.py                    # Kafka → HDFS raw-zone writer
├── spark/
│   ├── sql/
│   │   └── analytics.sql              # SQL analytics queries
│   ├── __pycache__/
│   ├── backend.py                     # FastAPI service exposing model/analytics results
│   ├── cleaning.py                    # Raw data cleaning
│   ├── customer_churn_prediction.py   # Churn prediction model
│   ├── customer_segmentation.py       # Customer segmentation model
│   ├── explore.py                     # Exploratory data analysis
│   ├── feature_engineering.py         # Feature engineering pipeline
│   └── transformation.py              # Data transformation/aggregation logic
├── app.py                             # Streamlit dashboard
├── consumer.Dockerfile                # Image for the Kafka consumer service
├── producer.Dockerfile                # Image for the Kafka producer service
├── Dockerfile.spark                   # Image for Spark master/worker
├── docker-compose.yml                 # Full stack orchestration
└── .gitignore
```

## Prerequisites

- Docker Engine 20.10+ and Docker Compose v2
- ~4 GB of free RAM available to Docker (Hadoop + Kafka + Spark together)
- Python 3.9+ and `pip` (for running the Streamlit app locally, outside Docker)

## Getting Started

1. **Clone the repository**
   ```bash
   git clone <your-repo-url>
   cd final_proj
   ```

2. **Add source data**
   Place the six CSVs in `data/`: `customers.csv`, `products.csv`, `stores.csv`, `employees.csv`, `discounts.csv`, `transactions.csv`.

3. **Build and start the backend stack**
   ```bash
   docker-compose up --build
   ```
   This starts, in dependency order:
   - `namenode` / `datanode` — HDFS storage
   - `kafka` — the message broker
   - `producer` — streams the CSVs into Kafka once, then exits
   - `consumer` — writes Kafka data into HDFS, then exits once idle
   - `spark-master` / `spark-worker` — the Spark cluster, with the FastAPI backend (`backend.py`) launched automatically on the master at port `8000`

4. **Run the Spark jobs** (cleaning, feature engineering, ML, analytics) against the ingested HDFS data — for example:
   ```bash
   docker exec -it spark-master /opt/spark/bin/spark-submit /opt/spark/apps/transformation.py
   docker exec -it spark-master /opt/spark/bin/spark-submit /opt/spark/apps/customer_segmentation.py
   docker exec -it spark-master /opt/spark/bin/spark-submit /opt/spark/apps/customer_churn_prediction.py
   ```

5. **Launch the dashboard**
   ```bash
   pip install streamlit
   streamlit run app.py
   ```
   The app reads model and analytics results (typically via the FastAPI backend at `http://localhost:8000`).

## Services & Ports

| Service        | Container       | Ports                    | Purpose                              |
|----------------|-----------------|---------------------------|----------------------------------------|
| HDFS NameNode  | `namenode`      | `9870` (UI), `9000` (RPC) | HDFS metadata / web UI                |
| HDFS DataNode  | `datanode`      | `9864`                    | HDFS block storage                    |
| Kafka Broker   | `kafka`         | `9092`                    | Message broker (KRaft, single node)   |
| Kafka Producer | `kafka-producer`| —                          | One-shot CSV → Kafka publisher        |
| Kafka Consumer | `kafka-consumer`| —                          | Kafka → HDFS raw-zone writer          |
| Spark Master   | `spark-master`  | `8080` (UI), `7077` (RPC), `8000` (API) | Spark master + FastAPI backend |
| Spark Worker   | `spark-worker`  | `8081` (UI)               | Spark worker (2 cores / 1 GB memory)  |

## Data

| File               | Description                              |
|--------------------|--------------------------------------------|
| `customers.csv`    | Customer master data                        |
| `products.csv`     | Product catalog                             |
| `stores.csv`       | Store locations/details                     |
| `employees.csv`    | Employee/staff records                      |
| `discounts.csv`    | Discount and promotion data                 |
| `transactions.csv` | Point-of-sale transaction records           |

## Kafka Layer

Kafka acts as a durable transport layer between the CSV files and HDFS. Both the producer and consumer are designed to run to completion and exit once the batch load is done.

- Each CSV maps 1:1 to a Kafka topic of the same name: `customers`, `products`, `stores`, `employees`, `discounts`, `transactions`.
- The producer serializes every row as UTF-8 JSON and flushes every 10,000 messages, logging progress as it goes, then closes the connection once all six files are sent.
- The consumer joins all six topics under the consumer group `fashion-hdfs-consumer-v2`, with `enable_auto_commit=False` — offsets advance only after data is durably written to HDFS.
- Both services are lightweight Python containers built on `python:3.11-slim`, with dependencies installed directly in the Dockerfile — no separate `requirements.txt` is used.

  `producer.Dockerfile`:
  ```dockerfile
  FROM python:3.11-slim
  WORKDIR /app
  COPY kafka/producer.py /app/producer.py
  RUN pip install --no-cache-dir kafka-python
  CMD ["python", "/app/producer.py"]
  ```

  `consumer.Dockerfile`:
  ```dockerfile
  FROM python:3.11-slim
  WORKDIR /app
  COPY kafka/consumer.py /app/consumer.py
  RUN pip install --no-cache-dir kafka-python requests
  CMD ["python", "-u", "/app/consumer.py"]
  ```

## HDFS Layer

Raw ingested data is written as JSON Lines under:
```
/data/raw/<topic>/part-<partition>-<part_number>.json
```
Example: `/data/raw/transactions/part-0-00003.json`

## Spark Layer

The Spark cluster runs on the official `apache/spark:3.5.6` image (`Dockerfile.spark`), extended with the Python dependencies needed for data processing, ML, and the API layer:

```dockerfile
FROM apache/spark:3.5.6

USER root

# Install Python dependencies
RUN python3 -m pip install --no-cache-dir \
    numpy \
    pandas \
    fastapi \
    "uvicorn[standard]"

# Make PySpark available to Python
ENV PYTHONPATH="/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"

USER spark
```

Job scripts, located in `spark/`, are run as standalone `spark-submit` jobs against the cluster (`spark-master:7077`). They split into two output paths:

**Feed the Streamlit app (via `backend.py`):**

| Script                              | Purpose                                   |
|--------------------------------------|--------------------------------------------|
| `cleaning.py`                        | Cleans and validates raw HDFS data         |
| `transformation.py`                  | Transforms/joins datasets for downstream use |
| `feature_engineering.py`             | Builds model-ready features                |
| `customer_segmentation.py`           | Segments customers (e.g., clustering)      |
| `customer_churn_prediction.py`       | Predicts customer churn                    |
| `explore.py`                         | Ad hoc exploratory analysis                |

**Feeds the Power BI dashboard:**

| Script                              | Purpose                                   |
|--------------------------------------|--------------------------------------------|
| `sql/analytics.sql`                  | SQL analytics queries run via Spark SQL, exported as the Power BI data source |

## API & Dashboard

- **`backend.py`** — a FastAPI application launched with Uvicorn inside the `spark-master` container (`http://localhost:8000`), exposing endpoints for the ML model results (segmentation, churn prediction).
- **`app.py`** — a Streamlit dashboard that consumes the backend API to display churn predictions and customer segments.
- **Power BI** — a separate dashboard connected directly to the output of `analytics.sql`, used for business-facing sales/analytics reporting.

## Environment Variables

| Variable         | Used by            | Description                                  |
|-------------------|--------------------|-----------------------------------------------|
| `CORE_CONF_fs_defaultFS` | `namenode`, `datanode` | HDFS default filesystem URI (`hdfs://namenode:9000`) |
| `HADOOP_CONF_DIR`  | `consumer`, `spark-master`, `spark-worker` | Path to mounted Hadoop client configuration |
| `SPARK_WORKER_MEMORY` | `spark-worker`  | Memory allocated to the Spark worker (`1G`)   |
| `SPARK_WORKER_CORES`  | `spark-worker`  | CPU cores allocated to the Spark worker (`2`) |

## Troubleshooting

- **Producer/consumer can't connect to Kafka** — make sure the `kafka` container is healthy before the producer/consumer start; `depends_on` only waits for container start, not readiness, so a retry loop may be needed on slow machines.
- **HDFS `MKDIRS`/`CREATE` errors** — confirm `namenode` and `datanode` are both up and that `hadoop-config/` contains valid client configuration.
- **Consumer exits immediately** — this is expected once Kafka has been idle for 10 seconds; it means ingestion is complete, not that it crashed.
- **Streamlit can't reach the backend** — verify `spark-master` is running and port `8000` is reachable at `http://localhost:8000`.

## Power BI Dashboard

The **Power BI** dashboard is a dedicated reporting layer connected to the output of `spark/sql/analytics.sql` — it does not include the ML model results (those live in the Streamlit app instead).

- **Data source:** the aggregated results produced by running `analytics.sql` against the cleaned data in HDFS.
- **Suggested pages:**
  - Sales & revenue overview by store, product category, and time period
  - Discount/promotion performance
  - Store and employee performance breakdowns
- **Setup:**
  1. Open the `.pbix` file in Power BI Desktop.
  2. Point the data source connection to the exported result of `analytics.sql` (e.g., a CSV/Parquet export from Spark, or a database the job writes to).
  3. Refresh the data model to load the latest results.
  4. Publish to Power BI Service if shared/online access is needed.

> Add the `.pbix` file to the repository (e.g., under a `powerbi/` folder) and update this section with its exact filename and data source once finalized.

## License

Add your license here (e.g., MIT).
