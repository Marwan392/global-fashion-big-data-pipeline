"""
FastAPI backend service running inside Docker to query the full HDFS dataset.
"""
from fastapi import FastAPI
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import math

app = FastAPI()

# Initialize Spark session pointing to the master or letting it use the environment defaults
spark = SparkSession.builder \
    .appName("GlobalFashionAnalytics") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

FEATURE_PATH = "hdfs://namenode:9000/data/processed/engineered_features"

@app.get("/summary")
def get_summary():
    df = spark.read.parquet(FEATURE_PATH)
    total_rows = df.count()
    total_spend = df.agg({"CustomerTotalSpend": "sum"}).collect()[0][0]
    avg_profit = df.agg({"CustomerAvgProfit": "mean"}).collect()[0][0]
    
    # Corrected column name with a space: "Customer ID"
    unique_customers = df.select("Customer ID").distinct().count()

    return {
        "total_rows": total_rows,
        "total_spend": total_spend,
        "avg_profit": avg_profit,
        "unique_customers": unique_customers
    }

@app.get("/data")
def get_full_data():
    df = spark.read.parquet(FEATURE_PATH)
    
    # Select only the exact columns needed for the charts to keep payloads lightning fast
    subset_df = df.select("Customer ID", "CustomerTotalSpend", "CustomerRecencyDays").limit(2000)
    pdf = subset_df.toPandas()
    
    records = pdf.to_dict(orient="records")
    
    cleaned_records = []
    for row in records:
        cleaned_row = {}
        for k, v in row.items():
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                cleaned_row[k] = None
            else:
                cleaned_row[k] = v
        cleaned_records.append(cleaned_row)
        
    return cleaned_records
