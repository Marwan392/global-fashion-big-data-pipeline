import os
import re
import time
import math
import threading
from decimal import Decimal
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pyspark.sql import SparkSession, functions as F

HDFS_URI = os.getenv("HDFS_URI", "hdfs://namenode:9000")
FACT_PATH = f"{HDFS_URI}/data/processed/fact_transactions"
CHURN_PATH = f"{HDFS_URI}/data/processed/churn_predictions"
ANALYTICS_SQL_PATH = os.getenv(
    "ANALYTICS_SQL_FILE",
    "/opt/spark/apps/sql/analytics.sql"
)

EXPECTED_SQL_QUERIES = 15
CACHE_TTL = 120
MAX_SQL_ROWS = 5000

app = FastAPI(
    title="Global Fashion Retail Analytics API",
    version="3.0.0"
)

spark = (
    SparkSession.builder
    .appName("GlobalFashionRetailAnalyticsAPI")
    .master("spark://spark-master:7077")
    .config("spark.executor.memory", "3g")
    .config("spark.executor.cores", "4")
    .config("spark.cores.max", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

cache = {}
cache_lock = threading.Lock()

def cached(key, loader):
    now = time.time()
    with cache_lock:
        item = cache.get(key)
    if item and now - item["time"] < CACHE_TTL:
        return item["value"]
    value = loader()
    with cache_lock:
        cache[key] = {"time": time.time(), "value": value}
    return value

def read_parquet(path):
    try:
        return spark.read.parquet(path)
    except Exception as exc:
        raise HTTPException(503, f"Unable to read dataset: {path}") from exc

def clean_value(value):
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if hasattr(value, "item"):
        try:
            return clean_value(value.item())
        except Exception:
            pass
    return value

def dataframe_records(df, limit=MAX_SQL_ROWS):
    rows = df.limit(limit).toPandas().to_dict("records")
    return [
        {key: clean_value(value) for key, value in row.items()}
        for row in rows
    ]

SQL_QUERY_DEFINITIONS = [
    ("data_overview", "Basic Data Overview"),
    ("sales_profit", "Total Sales & Profit"),
    ("sales_by_year", "Sales by Year"),
    ("monthly_sales", "Monthly Sales Trend"),
    ("category_sales", "Sales by Product Category"),
    ("subcategory_sales", "Sales by Product Sub-Category"),
    ("top_products", "Top 10 Products by Revenue"),
    ("customer_country_sales", "Sales by Customer Country"),
    ("store_country_sales", "Sales by Store Country"),
    ("payment_methods", "Payment Method Analysis"),
    ("gender_analysis", "Customer Gender Analysis"),
    ("weekday_weekend", "Weekday vs Weekend"),
    ("discount_analysis", "Discount Analysis"),
    ("top_customers", "Top 10 Customers by Revenue"),
    ("top_stores", "Top 10 Stores by Revenue"),
]

SQL_QUERY_MAP = dict(SQL_QUERY_DEFINITIONS)

def split_sql_statements(sql_text):
    statements, buffer, quote = [], [], None
    i = 0

    while i < len(sql_text):
        char = sql_text[i]

        if quote is None:
            if char in ("'", '"', "`"):
                quote = char
                buffer.append(char)
            elif char == ";":
                statement = "".join(buffer).strip()
                if statement:
                    statements.append(statement)
                buffer = []
            else:
                buffer.append(char)
        else:
            buffer.append(char)
            if char == quote:
                if quote == "'" and i + 1 < len(sql_text) and sql_text[i + 1] == "'":
                    buffer.append(sql_text[i + 1])
                    i += 1
                else:
                    quote = None
        i += 1

    statement = "".join(buffer).strip()
    if statement:
        statements.append(statement)

    return statements

def load_analytics_sql():
    path = Path(ANALYTICS_SQL_PATH)

    if not path.exists():
        raise HTTPException(
            503,
            f"analytics.sql was not found at {ANALYTICS_SQL_PATH}. "
            "Make sure ./spark is mounted to /opt/spark/apps."
        )

    try:
        return path.read_text(encoding="utf-8")
    except Exception as exc:
        raise HTTPException(503, f"Unable to read analytics.sql: {exc}") from exc

def extract_sql_queries():
    if not os.path.exists(ANALYTICS_SQL_PATH):
        raise HTTPException(404, f"SQL file not found: {ANALYTICS_SQL_PATH}")

    with open(ANALYTICS_SQL_PATH, "r", encoding="utf-8") as f:
        sql = re.sub(r"--.*", "", f.read())

    statements = [s.strip() for s in sql.split(";") if s.strip()]

    create_views = [
        s for s in statements
        if re.match(r"^\s*CREATE\s+OR\s+REPLACE\s+TEMP\s+VIEW\b", s, re.I)
    ]

    select_queries = [
        s for s in statements
        if re.match(r"^\s*SELECT\b", s, re.I)
    ]

    if len(select_queries) != EXPECTED_SQL_QUERIES:
        raise HTTPException(
            500,
            f"analytics.sql contains {len(select_queries)} SELECT statements, "
            f"but the dashboard expects {EXPECTED_SQL_QUERIES}."
        )

    return create_views, select_queries

def execute_analytics_sql():
    create_views, select_queries = extract_sql_queries()

    for view_sql in create_views:
        spark.sql(view_sql)

    results = {}

    for i, query in enumerate(select_queries, 1):
        df = spark.sql(query).limit(MAX_SQL_ROWS)
        rows = dataframe_records(df)

        results[f"query_{i}"] = {
            "title": f"Analytics Query {i}",
            "columns": df.columns,
            "rows": rows,
            "row_count": len(rows)
        }

    return {
        "query_count": len(results),
        "queries": results
    }

@app.get("/")
def root():
    return {
        "status": "running",
        "service": "Global Fashion Retail Analytics API",
        "version": "3.0.0"
    }

@app.get("/health")
def health():
    return {"status": "healthy"}

@app.get("/sql-health")
def sql_health():
    create_views, select_queries = extract_sql_queries()
    return {
        "status": "healthy",
        "sql_file": ANALYTICS_SQL_PATH,
        "sql_file_exists": Path(ANALYTICS_SQL_PATH).exists(),
        "temp_views": len(create_views),
        "select_queries": len(select_queries),
        "expected_queries": len(SQL_QUERY_DEFINITIONS)
    }

@app.get("/summary")
def summary():
    def load():
        row = (
            read_parquet(FACT_PATH)
            .agg(
                F.count("*").alias("transactions"),
                F.sum("Line Total USD").alias("revenue"),
                F.sum("Profit USD").alias("profit"),
                F.countDistinct("Customer ID").alias("customers"),
                F.countDistinct("Invoice ID").alias("orders")
            )
            .first()
        )

        revenue = float(row["revenue"] or 0)
        profit = float(row["profit"] or 0)

        return {
            "total_transactions": int(row["transactions"] or 0),
            "total_revenue": revenue,
            "total_profit": profit,
            "profit_margin": profit / revenue if revenue else 0,
            "unique_customers": int(row["customers"] or 0),
            "unique_orders": int(row["orders"] or 0)
        }

    return cached("summary", load)

@app.get("/customer-analytics")
def customer_analytics():
    def load():
        row = (
            read_parquet(CHURN_PATH)
            .agg(
                F.sum(F.when(F.col("RiskLevel") == "HIGH", 1).otherwise(0)).alias("high"),
                F.sum(F.when(F.col("RiskLevel") == "MEDIUM", 1).otherwise(0)).alias("medium"),
                F.sum(F.when(F.col("RiskLevel") == "LOW", 1).otherwise(0)).alias("low"),
                F.sum(F.when(F.col("ChurnLabel") == 1, 1).otherwise(0)).alias("churned"),
                F.sum(F.when(F.col("ChurnLabel") == 0, 1).otherwise(0)).alias("active")
            )
            .first()
        )

        return {
            "risk_distribution": [
                {"RiskLevel": "HIGH", "count": int(row["high"] or 0)},
                {"RiskLevel": "MEDIUM", "count": int(row["medium"] or 0)},
                {"RiskLevel": "LOW", "count": int(row["low"] or 0)}
            ],
            "churn_distribution": [
                {"ChurnLabel": 1, "count": int(row["churned"] or 0)},
                {"ChurnLabel": 0, "count": int(row["active"] or 0)}
            ]
        }

    return cached("customer-analytics", load)

@app.get("/churn-summary")
def churn_summary():
    def load():
        row = (
            read_parquet(CHURN_PATH)
            .agg(
                F.count("*").alias("total"),
                F.sum(F.when(F.col("PredictedChurn") == 1, 1).otherwise(0)).alias("churn"),
                F.avg("ChurnProbability").alias("probability"),
                F.sum(F.when(F.col("RiskLevel") == "HIGH", 1).otherwise(0)).alias("high"),
                F.sum(F.when(F.col("RiskLevel") == "MEDIUM", 1).otherwise(0)).alias("medium"),
                F.sum(F.when(F.col("RiskLevel") == "LOW", 1).otherwise(0)).alias("low")
            )
            .first()
        )

        return {
            "total_customers": int(row["total"] or 0),
            "predicted_churn_customers": int(row["churn"] or 0),
            "average_churn_probability": float(row["probability"] or 0),
            "high_risk_customers": int(row["high"] or 0),
            "medium_risk_customers": int(row["medium"] or 0),
            "low_risk_customers": int(row["low"] or 0)
        }

    return cached("churn-summary", load)

@app.get("/high-risk-customers")
def high_risk_customers():
    def load():
        df = (
            read_parquet(CHURN_PATH)
            .filter(F.col("RiskLevel") == "HIGH")
            .select(
                "Customer ID",
                "ChurnProbability",
                "RiskLevel",
                "PredictedChurn"
            )
            .orderBy(F.desc("ChurnProbability"))
            .limit(100)
        )
        return dataframe_records(df, 100)

    return cached("high-risk-customers", load)

@app.get("/churn-distribution")
def churn_distribution():
    def load():
        df = (
            read_parquet(CHURN_PATH)
            .select(
                "Customer ID",
                "ChurnProbability",
                "RiskLevel"
            )
            .orderBy(F.desc("ChurnProbability"))
            .limit(2000)
        )
        return dataframe_records(df, 2000)

    return cached("churn-distribution", load)

@app.get("/sql-analytics")
def sql_analytics():
    return cached("sql-analytics", execute_analytics_sql)

@app.get("/sql-analytics/{query_name}")
def sql_analytics_query(query_name: str):
    if query_name not in SQL_QUERY_MAP:
        raise HTTPException(
            404,
            f"Unknown query '{query_name}'. Available queries: {', '.join(SQL_QUERY_MAP)}"
        )

    return cached(
        "sql-analytics",
        execute_analytics_sql
    )["queries"][query_name]
