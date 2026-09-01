import math
from fastapi import FastAPI
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

app = FastAPI(
    title="Global Fashion Retail Analytics API",
    description="API for Big Data Analytics and Customer Churn Prediction",
    version="1.0.0"
)

spark = SparkSession.builder.appName("GlobalFashionAnalyticsBackend").master("spark://spark-master:7077").getOrCreate()

HDFS_URI = "hdfs://namenode:9000"
FACT_PATH = f"{HDFS_URI}/data/processed/fact_transactions"
FEATURE_PATH = f"{HDFS_URI}/data/processed/engineered_features"
CHURN_PREDICTIONS_PATH = f"{HDFS_URI}/data/processed/churn_predictions"

def clean_value(value):
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value

def dataframe_to_records(df):
    return [{k: clean_value(v) for k, v in row.items()} for row in df.toPandas().to_dict(orient="records")]

@app.get("/")
def root():
    return {"status": "running", "service": "Global Fashion Retail Analytics API"}

@app.get("/health")
def health():
    return {"status": "healthy"}

@app.get("/summary")
def get_summary():
    df = spark.read.parquet(FACT_PATH)
    summary = df.agg(
        F.count("*").alias("total_transactions"),
        F.sum("Line Total USD").alias("total_revenue"),
        F.sum("Profit USD").alias("total_profit"),
        F.countDistinct("Customer ID").alias("unique_customers"),
        F.countDistinct("Invoice ID").alias("unique_orders")
    ).first()
    
    total_revenue = summary["total_revenue"] if summary["total_revenue"] is not None else 0.0
    total_profit = summary["total_profit"] if summary["total_profit"] is not None else 0.0
    profit_margin = total_profit / total_revenue if total_revenue > 0 else 0.0
    
    return {
        "total_transactions": summary["total_transactions"],
        "total_revenue": float(total_revenue),
        "total_profit": float(total_profit),
        "profit_margin": float(profit_margin),
        "unique_customers": summary["unique_customers"],
        "unique_orders": summary["unique_orders"]
    }

@app.get("/customer-analytics")
def customer_analytics():
    df = spark.read.parquet(CHURN_PREDICTIONS_PATH)
    risk_distribution = df.groupBy("RiskLevel").count().orderBy("RiskLevel")
    churn_distribution = df.groupBy("ChurnLabel").count().orderBy("ChurnLabel")
    return {
        "risk_distribution": dataframe_to_records(risk_distribution),
        "churn_distribution": dataframe_to_records(churn_distribution)
    }

@app.get("/churn-summary")
def churn_summary():
    df = spark.read.parquet(CHURN_PREDICTIONS_PATH)
    summary = df.agg(
        F.count("*").alias("total_customers"),
        F.sum(F.when(F.col("PredictedChurn") == 1, 1).otherwise(0)).alias("predicted_churn_customers"),
        F.avg("ChurnProbability").alias("average_churn_probability")
    ).first()
    
    return {
        "total_customers": summary["total_customers"],
        "predicted_churn_customers": summary["predicted_churn_customers"],
        "average_churn_probability": float(summary["average_churn_probability"] or 0.0),
        "high_risk_customers": df.filter(F.col("RiskLevel") == "HIGH").count(),
        "medium_risk_customers": df.filter(F.col("RiskLevel") == "MEDIUM").count(),
        "low_risk_customers": df.filter(F.col("RiskLevel") == "LOW").count()
    }

@app.get("/high-risk-customers")
def high_risk_customers():
    df = spark.read.parquet(CHURN_PREDICTIONS_PATH)
    high_risk_df = df.filter(F.col("RiskLevel") == "HIGH").orderBy(F.desc("ChurnProbability")).limit(100)
    return dataframe_to_records(high_risk_df)

@app.get("/churn-distribution")
def churn_distribution():
    df = spark.read.parquet(CHURN_PREDICTIONS_PATH)
    distribution = df.select("Customer ID", "ChurnProbability", "RiskLevel").orderBy(F.desc("ChurnProbability")).limit(2000)
    return dataframe_to_records(distribution)

@app.get("/model-performance")
def model_performance():
    return {
        "best_model": "LogisticRegression",
        "models": [
            {"name": "Logistic Regression", "roc_auc": 0.6400, "f1": 0.6103, "precision": 0.6536, "recall": 0.6724, "churn_precision": 0.6805, "churn_recall": 0.9358, "churn_f1": 0.7880},
            {"name": "Decision Tree", "roc_auc": 0.5815, "f1": 0.6157, "precision": 0.6517, "recall": 0.6725, "churn_precision": 0.6829, "churn_recall": 0.9270, "churn_f1": 0.7864},
            {"name": "Random Forest", "roc_auc": 0.6403, "f1": 0.6197, "precision": 0.6523, "recall": 0.6733, "churn_precision": 0.6849, "churn_recall": 0.9220, "churn_f1": 0.7860}
        ]
    }