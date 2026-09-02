# Global Fashion Retail — Pipeline Runbook

This documents the full working sequence to bring the project up from a
clean clone through to the live dashboard, based on the commands that have
actually worked in this environment.

An automated script (`run_pipeline.bat`) covers steps 1–9 below. The two
live services (backend API and Streamlit dashboard) need their own terminal
windows and are run manually at the end.

---

## 1. Clone and enter the project

```
git clone https://github.com/Marwan392/global-fashion-big-data-pipeline.git
cd global-fashion-big-data-pipeline
```

> **Avoid cloning under a OneDrive-synced folder** (e.g. `OneDrive\Desktop\...`).
> OneDrive's cloud-placeholder files can break Docker bind mounts with errors
> like `mkdir ... file exists`. Prefer a plain local path such as
> `C:\dev\global-fashion-big-data-pipeline`.

## 2. Start all containers

```
docker compose up -d
```

If containers were already running from a previous session, prefer a clean
slate to avoid leftover zombie processes:

```
docker compose down
docker compose up -d
```

## 3. Stream data into Kafka (producer)

```
docker compose run producer python3 /app/producer.py
```

This reads the source CSVs and publishes records to Kafka topics. It's safe
to let it run to completion, or stop it early with `Ctrl+C` for a smaller
test dataset — the traceback on interrupt is expected, not an error.

## 4. Consume from Kafka into HDFS (consumer)

```
docker compose run consumer python3 /app/consumer.py
```

## 5. Clear any leftover backend process

```
docker compose exec spark-master pkill -f uvicorn
```

This guards against a stale `uvicorn` process from a previous session still
holding port 8000, which causes `address already in use` errors later.

## 6. Fix HDFS ownership for Spark

```
docker exec -it namenode hdfs dfs -chown -R spark:supergroup /data
```

## 7. Run the Spark ETL pipeline, in order

```
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/apps/transformation.py
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/apps/feature_engineering.py
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/apps/customer_segmentation.py
```

## 8. Prepare the HDFS models directory

```
docker exec -it namenode hdfs dfs -mkdir -p /models
docker exec -it namenode hdfs dfs -chown spark:supergroup /models
docker exec -it namenode hdfs dfs -chmod 775 /models
```

## 9. Train the churn prediction model

```
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/apps/customer_churn_prediction.py
```

---

## Live services — run these in two separate terminals

**Terminal 1 — FastAPI backend:**
```
docker compose exec spark-master python3 -m uvicorn backend:app --app-dir /opt/spark/apps --host 0.0.0.0 --port 8000
```

**Terminal 2 — Streamlit dashboard:**
```
conda activate <environment name>
streamlit run app.py
```

---

## Dashboards & UIs

| Service | URL |
|---|---|
| Streamlit dashboard | http://localhost:8501/ |
| FastAPI backend | http://localhost:8000/ |
| Spark Master UI | http://localhost:8080/ |
| HDFS NameNode UI | http://localhost:9870/ |


