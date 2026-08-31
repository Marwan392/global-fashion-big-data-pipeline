"""
Spark cleaning job for the Global Fashion Big Data Pipeline.

Reads raw JSON files from HDFS at hdfs://namenode:9000/data/raw/<table>/*.json
(the Kafka consumer lands one JSON object per row there) and returns cleaned
Spark DataFrames — one per table — for transformation.py to consume directly.

This module does NOT write anything to disk/HDFS. It's meant to be imported:

    from cleaning import get_spark, clean_all
    spark = get_spark()
    clean_dfs = clean_all(spark)          # {"customers": df, "transactions": df, ...}
    customers_df = clean_dfs["customers"]

Every field in the raw JSON arrives as a STRING (the producer serializes CSV
rows with no type conversion), so all numeric/date casting below is mandatory.

Run standalone for a quick sanity check:
    /opt/spark/bin/spark-submit cleaning.py
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

HDFS_URI = os.environ.get("HDFS_URI", "hdfs://namenode:9000")
RAW_PATH = f"{HDFS_URI}/data/raw"

TABLES = ["customers", "products", "stores", "employees", "discounts", "transactions"]


def get_spark():
    return SparkSession.builder.appName("FashionDataCleaning").getOrCreate()


def read_raw(spark, table):
    """Read all JSON records for a table. Every column is force-cast to
    string, since a clean-looking batch can trick Spark's JSON reader into
    inferring a numeric/boolean type for what should be raw text."""
    path = f"{RAW_PATH}/{table}/*.json"
    df = spark.read.json(path)
    for c in df.columns:
        df = df.withColumn(c, F.col(c).cast(StringType()))
    return df


def to_null_if_blank(df: DataFrame, columns) -> DataFrame:
    """Trim strings and turn empty strings / null-like placeholders into
    real nulls. Case-insensitive — real data has been seen with 'NULL',
    'null', 'None', and 'NaN' all as literal placeholder strings."""
    for c in columns:
        df = df.withColumn(
            c,
            F.when(
                F.upper(F.trim(F.col(c))).isin("", "NAN", "NONE", "NULL"),
                None,
            ).otherwise(F.trim(F.col(c))),
        )
    return df


def report(df: DataFrame, name: str, before_count: int):
    after_count = df.count()
    print(f"[{name}] before={before_count:,} after={after_count:,} "
          f"dropped={before_count - after_count:,}", flush=True)


def remove_outliers_iqr(df: DataFrame, columns, factor: float = 1.5, label: str = "") -> DataFrame:
    """
    Classic IQR outlier filter. For each column, rows outside
    [Q1 - factor*IQR, Q3 + factor*IQR] are dropped. Nulls are left alone
    (earlier null checks already handle those).

    Uses approxQuantile — fast/distributed-friendly, good enough for bounds.
    """
    for c in columns:
        q1, q3 = df.approxQuantile(c, [0.25, 0.75], 0.01)
        iqr = q3 - q1
        lower, upper = q1 - factor * iqr, q3 + factor * iqr

        before = df.count()
        df = df.filter(F.col(c).isNull() | F.col(c).between(lower, upper))
        after = df.count()

        print(f"[{label}] outliers on '{c}': Q1={q1:.2f} Q3={q3:.2f} IQR={iqr:.2f} "
              f"bounds=[{lower:.2f}, {upper:.2f}] removed={before - after:,}", flush=True)
    return df


# ---------------------------------------------------------------------------
# Per-table cleaning
# ---------------------------------------------------------------------------

def clean_customers(df: DataFrame) -> DataFrame:
    df = to_null_if_blank(df, ["Name", "Email", "Telephone", "City", "Country", "Gender", "Job Title"])
    df = (
        df.withColumn("Customer ID", F.col("Customer ID").cast("int"))
        .withColumn("Date Of Birth", F.to_date("Date Of Birth", "yyyy-MM-dd"))
        .withColumn("Email", F.lower(F.col("Email")))
    )
    total = df.count()
    null_id_count = df.filter(F.col("Customer ID").isNull()).count()
    df = df.filter(F.col("Customer ID").isNotNull())

    before_dedup = df.count()
    df = df.dropDuplicates(["Customer ID"])
    dup_count = before_dedup - df.count()

    print(f"[customers] total={total:,} null_id_dropped={null_id_count:,} "
          f"duplicate_dropped={dup_count:,}", flush=True)
    return df


def clean_products(df: DataFrame) -> DataFrame:
    string_cols = ["Category", "Sub Category", "Description PT", "Description DE", "Description FR",
                   "Description ES", "Description EN", "Description ZH", "Color", "Sizes"]
    df = to_null_if_blank(df, string_cols)
    df = (
        df.withColumn("Product ID", F.col("Product ID").cast("int"))
        .withColumn("Production Cost", F.col("Production Cost").cast("double"))
        .withColumn("Sizes", F.split(F.col("Sizes"), "\\|"))  # "S|M|L" -> array
    )
    df = df.filter(
        F.col("Product ID").isNotNull()
        & F.col("Production Cost").isNotNull()
        & (F.col("Production Cost") >= 0)
    )
    df = df.dropDuplicates(["Product ID"])
    return remove_outliers_iqr(df, ["Production Cost"], factor=3, label="products")


def clean_stores(df: DataFrame) -> DataFrame:
    df = to_null_if_blank(df, ["Country", "City", "Store Name", "ZIP Code"])
    df = (
        df.withColumn("Store ID", F.col("Store ID").cast("int"))
        .withColumn("Number of Employees", F.col("Number of Employees").cast("int"))
        .withColumn("Latitude", F.col("Latitude").cast("double"))
        .withColumn("Longitude", F.col("Longitude").cast("double"))
    )
    df = df.filter(
        F.col("Store ID").isNotNull()
        & F.col("Latitude").between(-90, 90)
        & F.col("Longitude").between(-180, 180)
    )
    return df.dropDuplicates(["Store ID"])


def clean_employees(df: DataFrame) -> DataFrame:
    df = to_null_if_blank(df, ["Name", "Position"])
    df = (
        df.withColumn("Employee ID", F.col("Employee ID").cast("int"))
        .withColumn("Store ID", F.col("Store ID").cast("int"))
    )
    df = df.filter(F.col("Employee ID").isNotNull() & F.col("Store ID").isNotNull())
    return df.dropDuplicates(["Employee ID"])


def clean_discounts(df: DataFrame) -> DataFrame:
    df = to_null_if_blank(df, ["Description", "Category", "Sub Category"])
    df = (
        df.withColumn("Start", F.to_date("Start", "yyyy-MM-dd"))
        .withColumn("End", F.to_date("End", "yyyy-MM-dd"))
        .withColumnRenamed("Discont", "Discount")  # fix source typo, going forward
        .withColumn("Discount", F.col("Discount").cast("double"))
    )
    df = df.filter(
        F.col("Start").isNotNull()
        & F.col("End").isNotNull()
        & (F.col("End") >= F.col("Start"))
        & F.col("Discount").between(0, 1)  # fraction, e.g. 0.20 = 20%
    )
    return df.dropDuplicates()


def clean_transactions(df: DataFrame) -> DataFrame:
    string_cols = ["Invoice ID", "Size", "Color", "Currency", "Currency Symbol",
                   "SKU", "Transaction Type", "Payment Method"]
    df = to_null_if_blank(df, string_cols)
    df = (
        df.withColumn("Line", F.col("Line").cast("int"))
        .withColumn("Customer ID", F.col("Customer ID").cast("int"))
        .withColumn("Product ID", F.col("Product ID").cast("int"))
        .withColumn("Unit Price", F.col("Unit Price").cast("double"))
        .withColumn("Quantity", F.col("Quantity").cast("int"))
        .withColumn("Date", F.to_timestamp("Date", "yyyy-MM-dd HH:mm:ss"))  # has a time component
        .withColumn("Discount", F.col("Discount").cast("double"))
        .withColumn("Line Total", F.col("Line Total").cast("double"))
        .withColumn("Store ID", F.col("Store ID").cast("int"))
        .withColumn("Employee ID", F.col("Employee ID").cast("int"))
        .withColumn("Invoice Total", F.col("Invoice Total").cast("double"))
    )
    df = df.filter(
        F.col("Invoice ID").isNotNull()
        & F.col("Customer ID").isNotNull()
        & F.col("Product ID").isNotNull()
        & F.col("Discount").between(0, 1)
        & (F.col("Quantity") > 0)
        & (F.col("Unit Price") >= 0)
        & (F.col("Line Total") >= 0)
    )
    df = df.dropDuplicates(["Invoice ID", "Line"])  # line item = invoice + line number
    # NOTE: statistical IQR outlier removal was tried here and dropped ~25% of
    # rows — Unit Price / Line Total / Invoice Total are correlated and
    # naturally right-skewed (a few big/luxury orders are legitimate, not
    # bad data), so compounding IQR filters across all three over-pruned real
    # sales. Sticking to the hard validity checks above (positive price/qty/
    # totals, discount in [0,1]) until we have a domain-informed threshold.
    return df


CLEANERS = {
    "customers": clean_customers,
    "products": clean_products,
    "stores": clean_stores,
    "employees": clean_employees,
    "discounts": clean_discounts,
    "transactions": clean_transactions,
}


def clean_all(spark) -> dict:
    """Read + clean every table and return {table_name: cleaned_df}.
    This is the entry point transformation.py should import and call."""
    clean_dfs = {}
    for table in TABLES:
        print(f"\n=== Cleaning: {table} ===", flush=True)
        raw_df = read_raw(spark, table)
        before = raw_df.count()

        clean_df = CLEANERS[table](raw_df).cache()
        report(clean_df, table, before)

        clean_dfs[table] = clean_df
    return clean_dfs


if __name__ == "__main__":
    # Quick standalone sanity check — just prints counts/schemas, writes nothing.
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    dfs = clean_all(spark)
    for name, df in dfs.items():
        print(f"\n--- {name} schema ---")
        df.printSchema()

    spark.stop()