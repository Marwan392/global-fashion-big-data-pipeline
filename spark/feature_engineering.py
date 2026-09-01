"""
Feature Engineering Pipeline for the Global Fashion Retail Big Data Pipeline.

Reads the transaction-level fact table from HDFS and creates a
customer-level feature store for downstream ML models.

Input:
    /data/processed/fact_transactions

Output:
    /data/processed/engineered_features
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer


# ============================================================
# Configuration
# ============================================================

HDFS_URI = os.environ.get(
    "HDFS_URI",
    "hdfs://namenode:9000"
)

FACT_PATH = (
    f"{HDFS_URI}/data/processed/fact_transactions"
)

FEATURE_PATH = (
    f"{HDFS_URI}/data/processed/engineered_features"
)

# Reduce the number of small Parquet files in HDFS
OUTPUT_PARTITIONS = 8


# ============================================================
# Spark Session
# ============================================================

def get_spark():

    return (
        SparkSession.builder
        .appName("FashionRetailFeatureEngineering")
        .getOrCreate()
    )


# ============================================================
# Customer-Level Aggregations
# ============================================================

def build_customer_features(df):
    """
    Creates one row per customer with behavioral,
    financial, promotional, and demographic features.
    """

    # --------------------------------------------------------
    # Reference date
    # --------------------------------------------------------

    max_date = (
        df.select(
            F.max(F.to_date("Date")).alias("max_date")
        )
        .collect()[0]["max_date"]
    )

    print(
        f"[feature_engineering] Dataset reference date: {max_date}"
    )


    # --------------------------------------------------------
    # Customer aggregations
    # --------------------------------------------------------

    customer_df = (
        df.groupBy("Customer ID")
        .agg(

            # ================================================
            # Revenue
            # ================================================

            F.sum("Line Total USD")
            .alias("CustomerTotalRevenue"),


            # ================================================
            # Purchase Frequency
            # ================================================

            F.countDistinct("Invoice ID")
            .alias("CustomerPurchaseCount"),


            # ================================================
            # Quantity
            # ================================================

            F.sum("Quantity")
            .alias("CustomerTotalQuantity"),


            # ================================================
            # Average Order Value
            # ================================================

            (
                F.sum("Line Total USD")
                /
                F.countDistinct("Invoice ID")
            )
            .alias("CustomerAverageOrderValue"),


            # ================================================
            # Profit
            # ================================================

            F.sum("Profit USD")
            .alias("CustomerTotalProfit"),

            F.avg("Profit USD")
            .alias("CustomerAverageProfit"),


            # ================================================
            # Discount Behavior
            # ================================================

            F.avg("Discount")
            .alias("CustomerAverageDiscount"),


            # ================================================
            # Promo Behavior
            # ================================================

            F.avg(
                F.when(
                    F.col("Promo Active") == True,
                    1.0
                ).otherwise(0.0)
            )
            .alias("CustomerPromoPurchaseRate"),


            # ================================================
            # Weekend Behavior
            # ================================================

            F.avg(
                F.when(
                    F.col("Is Weekend") == True,
                    1.0
                ).otherwise(0.0)
            )
            .alias("CustomerWeekendPurchaseRate"),


            # ================================================
            # Last Purchase Date
            # ================================================

            F.max(
                F.to_date("Date")
            )
            .alias("CustomerLastPurchaseDate"),


            # ================================================
            # Customer Demographics
            # ================================================

            F.first(
                "Customer Country",
                ignorenulls=True
            )
            .alias("CustomerCountry"),

            F.first(
                "Customer Gender",
                ignorenulls=True
            )
            .alias("CustomerGender"),

            F.avg(
                "Customer Age At Purchase"
            )
            .alias("CustomerAge")

        )
    )


    # --------------------------------------------------------
    # Recency
    # --------------------------------------------------------

    customer_df = customer_df.withColumn(

        "CustomerRecencyDays",

        F.datediff(
            F.lit(max_date),
            F.col("CustomerLastPurchaseDate")
        )

    )


    # --------------------------------------------------------
    # Profit Margin
    # --------------------------------------------------------

    customer_df = customer_df.withColumn(

        "CustomerProfitMargin",

        F.when(
            F.col("CustomerTotalRevenue") > 0,

            F.col("CustomerTotalProfit")
            /
            F.col("CustomerTotalRevenue")

        ).otherwise(0.0)

    )


    # --------------------------------------------------------
    # Age Groups
    # --------------------------------------------------------

    customer_df = customer_df.withColumn(

        "AgeGroup",

        F.when(
            F.col("CustomerAge") < 25,
            "18-24"
        )

        .when(
            F.col("CustomerAge") < 35,
            "25-34"
        )

        .when(
            F.col("CustomerAge") < 45,
            "35-44"
        )

        .when(
            F.col("CustomerAge") < 55,
            "45-54"
        )

        .otherwise("55+")

    )


    return customer_df


# ============================================================
# Categorical Encoding
# ============================================================

def encode_categorical_features(df):
    """
    Converts categorical columns into numeric indexes
    suitable for Spark ML models.
    """

    columns_to_encode = [

        ("CustomerCountry", "CustomerCountryIndex"),
        ("CustomerGender", "CustomerGenderIndex"),
        ("AgeGroup", "AgeGroupIndex")

    ]


    for input_col, output_col in columns_to_encode:

        indexer = StringIndexer(

            inputCol=input_col,
            outputCol=output_col,
            handleInvalid="keep"

        )

        model = indexer.fit(df)

        df = model.transform(df)


    return df


# ============================================================
# Data Validation
# ============================================================

def validate_features(df):

    print(
        "\n[feature_engineering] Validating feature store..."
    )


    # Check duplicate customers

    duplicate_customers = (
        df.groupBy("Customer ID")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )


    print(
        f"[feature_engineering] Duplicate customers: "
        f"{duplicate_customers:,}"
    )


    # Check null values

    null_counts = df.select(

        *[
            F.sum(
                F.when(
                    F.col(column).isNull(),
                    1
                ).otherwise(0)
            )
            .alias(column)

            for column in df.columns
        ]

    )


    print(
        "\n[feature_engineering] Null value summary:"
    )

    null_counts.show(
        truncate=False
    )


# ============================================================
# Main Pipeline
# ============================================================

def run_feature_engineering(spark):

    print(
        f"\n[feature_engineering] Loading fact table from:\n"
        f"{FACT_PATH}\n"
    )


    # --------------------------------------------------------
    # Load fact table
    # --------------------------------------------------------

    df = spark.read.parquet(
        FACT_PATH
    )


    input_count = df.count()


    print(
        f"[feature_engineering] Input transaction rows: "
        f"{input_count:,}"
    )


    # --------------------------------------------------------
    # Build customer features
    # --------------------------------------------------------

    print(
        "\n[feature_engineering] Building customer features..."
    )


    features_df = build_customer_features(df)


    # --------------------------------------------------------
    # Handle missing numerical values
    # --------------------------------------------------------

    features_df = features_df.fillna({

        "CustomerTotalRevenue": 0.0,
        "CustomerPurchaseCount": 0,
        "CustomerTotalQuantity": 0,
        "CustomerAverageOrderValue": 0.0,
        "CustomerTotalProfit": 0.0,
        "CustomerAverageProfit": 0.0,
        "CustomerAverageDiscount": 0.0,
        "CustomerPromoPurchaseRate": 0.0,
        "CustomerWeekendPurchaseRate": 0.0,
        "CustomerRecencyDays": 0,
        "CustomerProfitMargin": 0.0,
        "CustomerAge": 0.0

    })


    # --------------------------------------------------------
    # Encode categorical variables
    # --------------------------------------------------------

    print(
        "[feature_engineering] Encoding categorical features..."
    )


    features_df = encode_categorical_features(
        features_df
    )


    # --------------------------------------------------------
    # Cache
    # --------------------------------------------------------

    features_df = features_df.cache()


    feature_count = features_df.count()


    print(
        f"\n[feature_engineering] Feature store rows: "
        f"{feature_count:,}"
    )


    # --------------------------------------------------------
    # Validation
    # --------------------------------------------------------

    validate_features(
        features_df
    )


    # --------------------------------------------------------
    # Show schema
    # --------------------------------------------------------

    print(
        "\n[feature_engineering] Feature store schema:"
    )


    features_df.printSchema()


    print(
        "\n[feature_engineering] Sample features:"
    )


    features_df.show(
        10,
        truncate=False
    )


    # --------------------------------------------------------
    # Reduce small files
    # --------------------------------------------------------

    output_df = features_df.coalesce(
        OUTPUT_PARTITIONS
    )


    # --------------------------------------------------------
    # Write feature store
    # --------------------------------------------------------

    print(
        f"\n[feature_engineering] Writing feature store to:\n"
        f"{FEATURE_PATH}"
    )


    (
        output_df.write
        .mode("overwrite")
        .parquet(FEATURE_PATH)
    )


    print(
        "\n[feature_engineering] "
        f"Successfully wrote {feature_count:,} customer feature rows."
    )


    features_df.unpersist()


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":

    spark = get_spark()

    spark.sparkContext.setLogLevel(
        "WARN"
    )


    try:

        run_feature_engineering(
            spark
        )

    finally:

        spark.stop()