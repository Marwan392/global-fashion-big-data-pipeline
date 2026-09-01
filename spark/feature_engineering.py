import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer

HDFS_URI = os.environ.get("HDFS_URI", "hdfs://namenode:9000")
FACT_PATH = f"{HDFS_URI}/data/processed/fact_transactions"
FEATURE_PATH = f"{HDFS_URI}/data/processed/engineered_features"
OUTPUT_PARTITIONS = 8

def get_spark():
    return SparkSession.builder.appName("FashionRetailFeatureEngineering").getOrCreate()

def build_customer_features(df):
    max_date = df.select(F.max(F.to_date("Date")).alias("max_date")).collect()[0]["max_date"]
    
    customer_df = df.groupBy("Customer ID").agg(
        F.sum("Line Total USD").alias("CustomerTotalRevenue"),
        F.countDistinct("Invoice ID").alias("CustomerPurchaseCount"),
        F.sum("Quantity").alias("CustomerTotalQuantity"),
        (F.sum("Line Total USD") / F.countDistinct("Invoice ID")).alias("CustomerAverageOrderValue"),
        F.sum("Profit USD").alias("CustomerTotalProfit"),
        F.avg("Profit USD").alias("CustomerAverageProfit"),
        F.avg("Discount").alias("CustomerAverageDiscount"),
        F.avg(F.when(F.col("Promo Active") == True, 1.0).otherwise(0.0)).alias("CustomerPromoPurchaseRate"),
        F.avg(F.when(F.col("Is Weekend") == True, 1.0).otherwise(0.0)).alias("CustomerWeekendPurchaseRate"),
        F.max(F.to_date("Date")).alias("CustomerLastPurchaseDate"),
        F.first("Customer Country", ignorenulls=True).alias("CustomerCountry"),
        F.first("Customer Gender", ignorenulls=True).alias("CustomerGender"),
        F.avg("Customer Age At Purchase").alias("CustomerAge")
    )
    
    customer_df = customer_df.withColumn("CustomerRecencyDays", F.datediff(F.lit(max_date), F.col("CustomerLastPurchaseDate")))
    customer_df = customer_df.withColumn("CustomerProfitMargin", F.when(F.col("CustomerTotalRevenue") > 0, F.col("CustomerTotalProfit") / F.col("CustomerTotalRevenue")).otherwise(0.0))
    customer_df = customer_df.withColumn("AgeGroup", 
        F.when(F.col("CustomerAge") < 25, "18-24")
        .when(F.col("CustomerAge") < 35, "25-34")
        .when(F.col("CustomerAge") < 45, "35-44")
        .when(F.col("CustomerAge") < 55, "45-54")
        .otherwise("55+")
    )
    return customer_df

def encode_categorical_features(df):
    columns_to_encode = [("CustomerCountry", "CustomerCountryIndex"), ("CustomerGender", "CustomerGenderIndex"), ("AgeGroup", "AgeGroupIndex")]
    for input_col, output_col in columns_to_encode:
        indexer = StringIndexer(inputCol=input_col, outputCol=output_col, handleInvalid="keep")
        df = indexer.fit(df).transform(df)
    return df

def validate_features(df):
    duplicate_customers = df.groupBy("Customer ID").count().filter(F.col("count") > 1).count()
    print(f"[feature_engineering] Duplicate customers: {duplicate_customers:,}")

def run_feature_engineering(spark):
    df = spark.read.parquet(FACT_PATH)
    features_df = build_customer_features(df)
    
    features_df = features_df.fillna({
        "CustomerTotalRevenue": 0.0, "CustomerPurchaseCount": 0, "CustomerTotalQuantity": 0,
        "CustomerAverageOrderValue": 0.0, "CustomerTotalProfit": 0.0, "CustomerAverageProfit": 0.0,
        "CustomerAverageDiscount": 0.0, "CustomerPromoPurchaseRate": 0.0, "CustomerWeekendPurchaseRate": 0.0,
        "CustomerRecencyDays": 0, "CustomerProfitMargin": 0.0, "CustomerAge": 0.0
    })
    
    features_df = encode_categorical_features(features_df).cache()
    feature_count = features_df.count()
    validate_features(features_df)
    
    features_df.coalesce(OUTPUT_PARTITIONS).write.mode("overwrite").parquet(FEATURE_PATH)
    features_df.unpersist()

if __name__ == "__main__":
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        run_feature_engineering(spark)
    finally:
        spark.stop()