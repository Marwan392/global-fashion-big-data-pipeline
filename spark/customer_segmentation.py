import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import StorageLevel
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

HDFS_URI = os.environ.get("HDFS_URI", "hdfs://namenode:9000")
FEATURE_PATH = f"{HDFS_URI}/data/processed/engineered_features"
SEGMENT_PATH = f"{HDFS_URI}/data/processed/customer_segments"

def get_spark():
    return SparkSession.builder.appName("FashionRetailCustomerSegmentation").config("spark.sql.shuffle.partitions", "16").config("spark.default.parallelism", "16").getOrCreate()

FEATURE_COLUMNS = [
    "CustomerTotalRevenue", "CustomerAverageOrderValue", "CustomerTotalProfit", "CustomerProfitMargin",
    "CustomerPurchaseCount", "CustomerTotalQuantity", "CustomerAverageDiscount", "CustomerPromoPurchaseRate",
    "CustomerWeekendPurchaseRate", "CustomerRecencyDays"
]

def prepare_features(df):
    ml_df = df.select("Customer ID", *FEATURE_COLUMNS)
    assembler = VectorAssembler(inputCols=FEATURE_COLUMNS, outputCol="features", handleInvalid="skip")
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=False)
    return scaler.fit(assembler.transform(ml_df)).transform(assembler.transform(ml_df))

def find_best_k(sample_df):
    evaluator = ClusteringEvaluator(featuresCol="scaled_features", predictionCol="prediction", metricName="silhouette", distanceMeasure="squaredEuclidean")
    results = []
    for k in range(2, 7):
        kmeans = KMeans(k=k, seed=42, featuresCol="scaled_features", predictionCol="prediction", maxIter=30, tol=1e-4)
        model = kmeans.fit(sample_df)
        score = evaluator.evaluate(model.transform(sample_df))
        results.append((k, score))
    best_k = max(results, key=lambda x: x[1])[0]
    return best_k, results

def train_final_model(df, k):
    kmeans = KMeans(k=k, seed=42, featuresCol="scaled_features", predictionCol="Cluster", maxIter=50, tol=1e-4)
    model = kmeans.fit(df)
    return model.transform(df), model

def analyze_clusters(df):
    return df.groupBy("Cluster").agg(
        F.count("*").alias("CustomerCount"),
        F.avg("CustomerTotalRevenue").alias("AvgRevenue"),
        F.avg("CustomerPurchaseCount").alias("AvgPurchaseCount"),
        F.avg("CustomerTotalQuantity").alias("AvgQuantity"),
        F.avg("CustomerAverageOrderValue").alias("AvgOrderValue"),
        F.avg("CustomerTotalProfit").alias("AvgProfit"),
        F.avg("CustomerAverageDiscount").alias("AvgDiscount"),
        F.avg("CustomerPromoPurchaseRate").alias("AvgPromoRate"),
        F.avg("CustomerWeekendPurchaseRate").alias("AvgWeekendRate"),
        F.avg("CustomerRecencyDays").alias("AvgRecencyDays")
    ).orderBy("Cluster")

def run_customer_segmentation(spark):
    df = spark.read.parquet(FEATURE_PATH)
    scaled_df = prepare_features(df)
    sample_df = scaled_df.sample(withReplacement=False, fraction=0.03, seed=42).persist(StorageLevel.MEMORY_AND_DISK)
    sample_df.count()
    
    best_k, results = find_best_k(sample_df)
    sample_df.unpersist()
    
    clustered_df, _ = train_final_model(scaled_df, best_k)
    analyze_clusters(clustered_df)
    
    output_df = clustered_df.select("Customer ID", *FEATURE_COLUMNS, "Cluster")
    output_df.repartition(8).write.mode("overwrite").parquet(SEGMENT_PATH)

if __name__ == "__main__":
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        run_customer_segmentation(spark)
    finally:
        spark.stop()