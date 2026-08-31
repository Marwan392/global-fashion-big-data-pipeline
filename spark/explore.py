"""
Scratch space for inspecting the fact table without touching
cleaning.py / transformation.py. Edit freely — this file isn't part
of the real pipeline.

Run:
    docker exec -it spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 /opt/spark/apps/explore.py
"""

from pyspark.sql import functions as F
from cleaning import get_spark, clean_all
from transformation import transform_all

spark = get_spark()
spark.sparkContext.setLogLevel("WARN")

clean_dfs = clean_all(spark)
fact_df = transform_all(clean_dfs).cache()


def section(title):
    print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")


# ---------------------------------------------------------------------------
# DATA QUALITY
# ---------------------------------------------------------------------------

section("Total rows")
total_rows = fact_df.count()
print(total_rows)

section("Null counts per column")
fact_df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c) for c in fact_df.columns
]).show(vertical=True, truncate=False)

section("Rows with negative profit (sold below production cost)")
neg_profit = fact_df.filter(F.col("Profit USD") < 0)
neg_count = neg_profit.count()
print(f"count: {neg_count:,}  ({neg_count / total_rows:.1%} of all rows)")
neg_profit.select("Invoice ID", "Product Category", "Promo Active", "Unit Price USD", "Production Cost", "Profit USD").show(5)

section("Rows where Promo Active but no discount recorded on the line (sanity check)")
fact_df.filter(F.col("Promo Active") & (F.col("Discount") == 0.0)).show(5)


# ---------------------------------------------------------------------------
# SALES OVERVIEW
# ---------------------------------------------------------------------------

section("Overall revenue / profit summary (USD)")
fact_df.agg(
    F.round(F.sum("Line Total USD"), 2).alias("Total Revenue"),
    F.round(F.sum("Profit USD"), 2).alias("Total Profit"),
    F.round(F.avg("Line Total USD"), 2).alias("Avg Line Value"),
    F.count("*").alias("Line Items"),
    F.countDistinct("Invoice ID").alias("Invoices"),
).show()

section("Revenue by store country")
fact_df.groupBy("Store Country").agg(
    F.round(F.sum("Line Total USD"), 2).alias("Total Revenue USD"),
    F.round(F.sum("Profit USD"), 2).alias("Total Profit USD"),
    F.count("*").alias("Transactions"),
).orderBy(F.desc("Total Revenue USD")).show()

section("Revenue by payment method")
fact_df.groupBy("Payment Method").agg(
    F.round(F.sum("Line Total USD"), 2).alias("Total Revenue USD"),
    F.count("*").alias("Transactions"),
).orderBy(F.desc("Total Revenue USD")).show()

section("Revenue by month (seasonality check)")
fact_df.groupBy("Purchase Year", "Purchase Month").agg(
    F.round(F.sum("Line Total USD"), 2).alias("Total Revenue USD"),
    F.count("*").alias("Transactions"),
).orderBy("Purchase Year", "Purchase Month").show(24)

section("Weekend vs weekday revenue")
fact_df.groupBy("Is Weekend").agg(
    F.round(F.sum("Line Total USD"), 2).alias("Total Revenue USD"),
    F.round(F.avg("Line Total USD"), 2).alias("Avg Line Value USD"),
    F.count("*").alias("Transactions"),
).show()


# ---------------------------------------------------------------------------
# PRODUCTS
# ---------------------------------------------------------------------------

section("Distinct product categories")
fact_df.select("Product Category").distinct().show(truncate=False)

section("Top 10 product sub-categories by revenue")
fact_df.groupBy("Product Category", "Product Sub Category").agg(
    F.round(F.sum("Line Total USD"), 2).alias("Total Revenue USD"),
    F.count("*").alias("Units Sold"),
).orderBy(F.desc("Total Revenue USD")).show(10, truncate=False)

section("Profit margin % by category")
fact_df.groupBy("Product Category").agg(
    F.round(F.sum("Profit USD"), 2).alias("Total Profit USD"),
    F.round(F.sum("Line Total USD"), 2).alias("Total Revenue USD"),
    F.round((F.sum("Profit USD") / F.sum("Line Total USD")) * 100, 1).alias("Margin %"),
).orderBy("Margin %").show()


# ---------------------------------------------------------------------------
# CUSTOMERS
# ---------------------------------------------------------------------------

section("Revenue by customer gender")
fact_df.groupBy("Customer Gender").agg(
    F.round(F.sum("Line Total USD"), 2).alias("Total Revenue USD"),
    F.countDistinct("Customer ID").alias("Distinct Customers"),
).show()

section("Customer age distribution at time of purchase")
fact_df.select(
    F.min("Customer Age At Purchase").alias("Min Age"),
    F.round(F.avg("Customer Age At Purchase"), 1).alias("Avg Age"),
    F.max("Customer Age At Purchase").alias("Max Age"),
).show()
# flag implausible ages — negative means DOB parsed wrong or DOB is after purchase date
fact_df.filter((F.col("Customer Age At Purchase") < 0) | (F.col("Customer Age At Purchase") > 100)).select(
    "Customer ID", "Date Of Birth", "Date", "Customer Age At Purchase"
).show(10)

section("Top 10 customers by total spend (USD)")
fact_df.groupBy("Customer ID", "Customer Name").agg(
    F.round(F.sum("Line Total USD"), 2).alias("Total Spend USD"),
    F.countDistinct("Invoice ID").alias("Invoices"),
).orderBy(F.desc("Total Spend USD")).show(10, truncate=False)


# ---------------------------------------------------------------------------
# STORES & EMPLOYEES
# ---------------------------------------------------------------------------

section("Revenue by store")
fact_df.groupBy("Store Name", "Store City", "Store Country").agg(
    F.round(F.sum("Line Total USD"), 2).alias("Total Revenue USD"),
    F.count("*").alias("Transactions"),
).orderBy(F.desc("Total Revenue USD")).show(20, truncate=False)

section("Top 10 employees by revenue generated")
fact_df.groupBy("Employee Name", "Employee Position", "Store Name").agg(
    F.round(F.sum("Line Total USD"), 2).alias("Total Revenue USD"),
    F.count("*").alias("Transactions"),
).orderBy(F.desc("Total Revenue USD")).show(10, truncate=False)


# ---------------------------------------------------------------------------
# PROMOTIONS
# ---------------------------------------------------------------------------

section("Promo vs non-promo: revenue and margin")
fact_df.groupBy("Promo Active").agg(
    F.round(F.sum("Line Total USD"), 2).alias("Total Revenue USD"),
    F.round(F.avg("Line Total USD"), 2).alias("Avg Line Value USD"),
    F.round((F.sum("Profit USD") / F.sum("Line Total USD")) * 100, 1).alias("Margin %"),
    F.count("*").alias("Transactions"),
).show()

section("Distribution of promo discount percentages actually applied")
fact_df.filter(F.col("Promo Active")).groupBy("Promo Discount Pct").count().orderBy("Promo Discount Pct").show()


spark.stop()