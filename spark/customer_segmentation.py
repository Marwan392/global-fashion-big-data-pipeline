"""
Customer Segmentation Pipeline
for the Global Fashion Retail Big Data Pipeline.

Reads the customer-level feature store from HDFS,
prepares and scales customer behavioral features,
evaluates multiple K-Means models on a sample,
selects the best number of clusters,
trains the final model on the full dataset,
and writes customer segments to HDFS.

Input:
    /data/processed/engineered_features

Output:
    /data/processed/customer_segments
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark import StorageLevel

from pyspark.ml.feature import (
    VectorAssembler,
    StandardScaler
)

from pyspark.ml.clustering import KMeans

from pyspark.ml.evaluation import ClusteringEvaluator


# ============================================================
# Configuration
# ============================================================

HDFS_URI = os.environ.get(
    "HDFS_URI",
    "hdfs://namenode:9000"
)


FEATURE_PATH = (
    f"{HDFS_URI}/data/processed/engineered_features"
)


SEGMENT_PATH = (
    f"{HDFS_URI}/data/processed/customer_segments"
)


# ============================================================
# Spark Session
# ============================================================

def get_spark():

    return (
        SparkSession.builder
        .appName("FashionRetailCustomerSegmentation")

        .config(
            "spark.sql.shuffle.partitions",
            "16"
        )

        .config(
            "spark.default.parallelism",
            "16"
        )

        .getOrCreate()
    )


# ============================================================
# ML Features
# ============================================================

FEATURE_COLUMNS = [

    # Financial behavior
    "CustomerTotalRevenue",

    "CustomerAverageOrderValue",

    "CustomerTotalProfit",

    "CustomerProfitMargin",


    # Purchase behavior
    "CustomerPurchaseCount",

    "CustomerTotalQuantity",


    # Promotion behavior
    "CustomerAverageDiscount",

    "CustomerPromoPurchaseRate",


    # Shopping behavior
    "CustomerWeekendPurchaseRate",


    # Customer activity
    "CustomerRecencyDays"

]


# ============================================================
# Feature Preparation
# ============================================================

def prepare_features(df):

    print(
        "\n[customer_segmentation] "
        "Preparing ML features..."
    )


    # --------------------------------------------------------
    # Select only required columns
    # --------------------------------------------------------

    ml_df = df.select(

        "Customer ID",

        *FEATURE_COLUMNS

    )


    # --------------------------------------------------------
    # Assemble features
    # --------------------------------------------------------

    assembler = VectorAssembler(

        inputCols=FEATURE_COLUMNS,

        outputCol="features",

        handleInvalid="skip"

    )


    assembled_df = assembler.transform(
        ml_df
    )


    # --------------------------------------------------------
    # Scale features
    # --------------------------------------------------------

    scaler = StandardScaler(

        inputCol="features",

        outputCol="scaled_features",

        withStd=True,

        withMean=False

    )


    scaler_model = scaler.fit(
        assembled_df
    )


    scaled_df = scaler_model.transform(
        assembled_df
    )


    return scaled_df


# ============================================================
# Find Best K
# ============================================================

def find_best_k(sample_df):

    print(
        "\n[customer_segmentation] "
        "Evaluating K-Means models..."
    )


    evaluator = ClusteringEvaluator(

        featuresCol="scaled_features",

        predictionCol="prediction",

        metricName="silhouette",

        distanceMeasure="squaredEuclidean"

    )


    results = []


    # --------------------------------------------------------
    # Test cluster counts
    # --------------------------------------------------------

    for k in range(2, 7):

        print(
            f"\n[customer_segmentation] "
            f"Training K-Means with k={k}"
        )


        kmeans = KMeans(

            k=k,

            seed=42,

            featuresCol="scaled_features",

            predictionCol="prediction",

            maxIter=30,

            tol=1e-4

        )


        model = kmeans.fit(
            sample_df
        )


        predictions = model.transform(
            sample_df
        )


        silhouette_score = evaluator.evaluate(
            predictions
        )


        print(

            f"[customer_segmentation] "
            f"k={k} | "
            f"Silhouette Score={silhouette_score:.4f}"

        )


        results.append(

            (
                k,
                silhouette_score
            )

        )


    # --------------------------------------------------------
    # Best K
    # --------------------------------------------------------

    best_k = max(

        results,

        key=lambda x: x[1]

    )[0]


    print(

        f"\n[customer_segmentation] "
        f"Best K selected: {best_k}"

    )


    return best_k, results


# ============================================================
# Train Final Model
# ============================================================

def train_final_model(df, k):

    print(

        f"\n[customer_segmentation] "
        f"Training final model with k={k}"

    )


    kmeans = KMeans(

        k=k,

        seed=42,

        featuresCol="scaled_features",

        predictionCol="Cluster",

        maxIter=50,

        tol=1e-4

    )


    model = kmeans.fit(
        df
    )


    clustered_df = model.transform(
        df
    )


    return clustered_df, model


# ============================================================
# Cluster Analysis
# ============================================================

def analyze_clusters(df):

    print(

        "\n[customer_segmentation] "
        "Cluster summary:"

    )


    cluster_summary = (

        df.groupBy("Cluster")

        .agg(

            F.count("*")
            .alias("CustomerCount"),


            F.avg("CustomerTotalRevenue")
            .alias("AvgRevenue"),


            F.avg("CustomerPurchaseCount")
            .alias("AvgPurchaseCount"),


            F.avg("CustomerTotalQuantity")
            .alias("AvgQuantity"),


            F.avg("CustomerAverageOrderValue")
            .alias("AvgOrderValue"),


            F.avg("CustomerTotalProfit")
            .alias("AvgProfit"),


            F.avg("CustomerAverageDiscount")
            .alias("AvgDiscount"),


            F.avg("CustomerPromoPurchaseRate")
            .alias("AvgPromoRate"),


            F.avg("CustomerWeekendPurchaseRate")
            .alias("AvgWeekendRate"),


            F.avg("CustomerRecencyDays")
            .alias("AvgRecencyDays")

        )

        .orderBy("Cluster")

    )


    cluster_summary.show(
        truncate=False
    )


    return cluster_summary


# ============================================================
# Main Pipeline
# ============================================================

def run_customer_segmentation(spark):

    print(

        f"\n[customer_segmentation] "
        f"Loading feature store from:\n"
        f"{FEATURE_PATH}\n"

    )


    # --------------------------------------------------------
    # Load feature store
    # --------------------------------------------------------

    df = spark.read.parquet(
        FEATURE_PATH
    )


    customer_count = df.count()


    print(

        "[customer_segmentation] "
        f"Customer rows: {customer_count:,}"

    )


    # --------------------------------------------------------
    # Prepare ML features
    # --------------------------------------------------------

    scaled_df = prepare_features(
        df
    )


    scaled_count = scaled_df.count()


    print(

        "[customer_segmentation] "
        f"Prepared customer rows: {scaled_count:,}"

    )


    # --------------------------------------------------------
    # Create sample for K evaluation
    # --------------------------------------------------------

    print(

        "\n[customer_segmentation] "
        "Creating sample for K evaluation..."

    )


    sample_df = (

        scaled_df

        .sample(

            withReplacement=False,

            fraction=0.03,

            seed=42

        )

        .persist(
            StorageLevel.MEMORY_AND_DISK
        )

    )


    sample_count = sample_df.count()


    print(

        "[customer_segmentation] "
        f"Sample rows: {sample_count:,}"

    )


    # --------------------------------------------------------
    # Find best K
    # --------------------------------------------------------

    best_k, results = find_best_k(
        sample_df
    )


    # --------------------------------------------------------
    # Release sample memory
    # --------------------------------------------------------

    sample_df.unpersist()


    print(
        "\n[customer_segmentation] "
        "Released evaluation sample from memory."
    )


    # --------------------------------------------------------
    # Train final model
    # --------------------------------------------------------

    clustered_df, model = train_final_model(

        scaled_df,

        best_k

    )


    # --------------------------------------------------------
    # Analyze clusters
    # --------------------------------------------------------

    analyze_clusters(
        clustered_df
    )


    # --------------------------------------------------------
    # Select output columns
    # --------------------------------------------------------

    output_df = clustered_df.select(

        "Customer ID",

        *FEATURE_COLUMNS,

        "Cluster"

    )


    # --------------------------------------------------------
    # Write results
    # --------------------------------------------------------

    print(

        f"\n[customer_segmentation] "
        f"Writing customer segments to:\n"
        f"{SEGMENT_PATH}"

    )


    (

        output_df

        .repartition(8)

        .write

        .mode("overwrite")

        .parquet(SEGMENT_PATH)

    )


    print(

        "\n[customer_segmentation] "
        "Customer segmentation completed successfully."

    )


    # --------------------------------------------------------
    # Print evaluation results
    # --------------------------------------------------------

    print(

        "\n[customer_segmentation] "
        "K-Means evaluation results:"

    )


    for k, score in results:

        print(

            f"k={k} | "
            f"silhouette={score:.4f}"

        )


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":

    spark = get_spark()


    spark.sparkContext.setLogLevel(
        "WARN"
    )


    try:

        run_customer_segmentation(
            spark
        )

    finally:

        spark.stop()