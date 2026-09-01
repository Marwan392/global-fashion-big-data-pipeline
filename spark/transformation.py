"""
Spark transformation job for the Global Fashion Big Data Pipeline.

Takes the cleaned DataFrames from cleaning.py and joins them into a single
denormalized, transaction-level DataFrame with derived business columns.
This is the table feature_eng.py will build ML features on top of.

Usage (in-memory, e.g. from another Spark script in the same run):
    from cleaning import get_spark, clean_all
    from transformation import transform_all

    spark = get_spark()
    clean_dfs = clean_all(spark)
    fact_df = transform_all(clean_dfs)

Run standalone to build AND persist the fact table to HDFS as Parquet at
hdfs://namenode:9000/data/processed/fact_transactions — so downstream
scripts (explore.py, feature_eng.py) can just read it back instead of
re-running cleaning + transformation every time:

    /opt/spark/bin/spark-submit transformation.py

Then elsewhere:
    fact_df = spark.read.parquet(f"{HDFS_URI}/data/processed/fact_transactions")
"""

import os
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Fixed FX rates to USD. Approximate, applied uniformly regardless of
# transaction date. Swap this for a date-indexed rates table later if
# historical accuracy becomes important.
# ---------------------------------------------------------------------------
FX_RATES_TO_USD = {
    "USD": 1.0,
    "EUR": 1.08,
    "GBP": 1.27,
    "CNY": 0.14,
}

HDFS_URI = os.environ.get("HDFS_URI", "hdfs://namenode:9000")
PROCESSED_PATH = f"{HDFS_URI}/data/processed/fact_transactions"


def add_usd_columns(tx: DataFrame) -> DataFrame:
    """Add a FX Rate To USD column plus *_USD versions of the money columns."""
    rate_map = F.create_map([F.lit(x) for pair in FX_RATES_TO_USD.items() for x in pair])
    tx = tx.withColumn("FX Rate To USD", rate_map[F.col("Currency")])

    for col in ["Unit Price", "Line Total", "Invoice Total"]:
        tx = tx.withColumn(f"{col} USD", F.round(F.col(col) * F.col("FX Rate To USD"), 2))
    return tx


def join_customers(tx: DataFrame, customers: DataFrame) -> DataFrame:
    customers = customers.select(
        F.col("Customer ID"),
        F.col("Name").alias("Customer Name"),
        F.col("Email").alias("Customer Email"),
        F.col("City").alias("Customer City"),
        F.col("Country").alias("Customer Country"),
        F.col("Gender").alias("Customer Gender"),
        F.col("Date Of Birth"),
        F.col("Job Title").alias("Customer Job Title"),
    )
    return tx.join(customers, on="Customer ID", how="left")


def join_products(tx: DataFrame, products: DataFrame) -> DataFrame:
    products = products.select(
        F.col("Product ID"),
        F.col("Category").alias("Product Category"),
        F.col("Sub Category").alias("Product Sub Category"),
        F.col("Description EN").alias("Product Description"),
        F.col("Production Cost"),
    )
    return tx.join(products, on="Product ID", how="left")


def join_stores(tx: DataFrame, stores: DataFrame) -> DataFrame:
    stores = stores.select(
        F.col("Store ID"),
        F.col("Store Name"),
        F.col("City").alias("Store City"),
        F.col("Country").alias("Store Country"),
        F.col("Latitude").alias("Store Latitude"),
        F.col("Longitude").alias("Store Longitude"),
    )
    return tx.join(stores, on="Store ID", how="left")


def join_employees(tx: DataFrame, employees: DataFrame) -> DataFrame:
    employees = employees.select(
        F.col("Employee ID"),
        F.col("Name").alias("Employee Name"),
        F.col("Position").alias("Employee Position"),
    )
    return tx.join(employees, on="Employee ID", how="left")


def join_active_promos(tx: DataFrame, discounts: DataFrame) -> DataFrame:
    """
    discounts.csv is a promo calendar by Category/Sub Category + date range,
    NOT a per-transaction discount. A transaction is "under promo" if its
    product's Category/Sub Category and Date fall inside a promo window.

    A product can theoretically match more than one overlapping promo
    window, so after the join we keep only the best (highest discount)
    match per transaction line via a window function, rather than letting
    duplicate promo matches fan out into duplicate transaction rows.
    """
    promos = discounts.select(
        F.col("Category").alias("Promo Category"),
        F.col("Sub Category").alias("Promo Sub Category"),
        F.col("Start").alias("Promo Start"),
        F.col("End").alias("Promo End"),
        F.col("Discount").alias("Promo Discount Pct"),
    )

    joined = tx.join(
        promos,
        on=(
            (tx["Product Category"] == promos["Promo Category"])
            & (tx["Product Sub Category"] == promos["Promo Sub Category"])
            & (F.to_date(tx["Date"]) >= promos["Promo Start"])
            & (F.to_date(tx["Date"]) <= promos["Promo End"])
        ),
        how="left",
    )

    window = Window.partitionBy("Invoice ID", "Line").orderBy(F.col("Promo Discount Pct").desc_nulls_last())
    joined = (
        joined.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "Promo Category", "Promo Sub Category", "Promo Start", "Promo End")
    )

    joined = joined.withColumn("Promo Active", F.col("Promo Discount Pct").isNotNull())
    return joined


def add_derived_columns(tx: DataFrame) -> DataFrame:
    tx = tx.withColumn(
        "Customer Age At Purchase",
        F.floor(F.datediff(F.to_date(F.col("Date")), F.col("Date Of Birth")) / 365.25).cast("int"),
    )

    # Profit assumes Production Cost is already USD-denominated (no currency
    # field exists for it in products.csv).
    tx = tx.withColumn(
        "Profit USD",
        F.round(F.col("Line Total USD") - (F.col("Production Cost") * F.col("Quantity")), 2),
    )

    tx = (
        tx.withColumn("Purchase Day Of Week", F.dayofweek(F.col("Date")))  # 1=Sun ... 7=Sat
        .withColumn("Purchase Month", F.month(F.col("Date")))
        .withColumn("Purchase Year", F.year(F.col("Date")))
        .withColumn("Is Weekend", F.col("Purchase Day Of Week").isin(1, 7))
    )
    return tx


def transform_all(clean_dfs: dict) -> DataFrame:
    """Build the single denormalized fact table from the cleaned tables."""
    tx = clean_dfs["transactions"]

    tx = add_usd_columns(tx)
    tx = join_customers(tx, clean_dfs["customers"])
    tx = join_products(tx, clean_dfs["products"])
    tx = join_stores(tx, clean_dfs["stores"])
    tx = join_employees(tx, clean_dfs["employees"])
    tx = join_active_promos(tx, clean_dfs["discounts"])
    tx = add_derived_columns(tx)

    return tx


if __name__ == "__main__":
    from cleaning import get_spark, clean_all

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    clean_dfs = clean_all(spark)
    fact_df = transform_all(clean_dfs).cache()

    row_count = fact_df.count()
    print(f"\n[transformation] fact table row count: {row_count:,}")
    fact_df.printSchema()
    fact_df.show(5, truncate=False)

    print(f"\n[transformation] writing fact table to {PROCESSED_PATH} ...")
    (
        fact_df.write
        .mode("overwrite")
        .partitionBy("Purchase Year", "Purchase Month")  # cheap date-range reads later
        .parquet(PROCESSED_PATH)
    )
    print(f"[transformation] done — {row_count:,} rows written")

    spark.stop()