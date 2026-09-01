import os
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

HDFS_URI = os.environ.get("HDFS_URI", "hdfs://namenode:9000")
FACT_PATH = f"{HDFS_URI}/data/processed/fact_transactions"
PREDICTIONS_PATH = f"{HDFS_URI}/data/processed/churn_predictions"
MODEL_PATH = f"{HDFS_URI}/models/customer_churn_model"

RANDOM_SEED = 42
TRAIN_RATIO = 0.8
CHURN_WINDOW_DAYS = 90

def get_spark():
    return (
        SparkSession.builder.appName("FashionRetailCustomerChurnPrediction")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .getOrCreate()
    )

def get_feature_columns():
    return [
        "CustomerTotalRevenue", "CustomerPurchaseCount", "CustomerTotalQuantity",
        "CustomerAverageOrderValue", "CustomerTotalProfit", "CustomerAverageProfit",
        "CustomerAverageDiscount", "CustomerPromoPurchaseRate", "CustomerWeekendPurchaseRate",
        "CustomerRecencyDays", "CustomerProfitMargin"
    ]

def prepare_churn_dataset(df):
    df = df.withColumn("TransactionDate", F.to_date("Date")).filter(F.col("TransactionDate").isNotNull())
    max_date = df.agg(F.max("TransactionDate").alias("max_date")).first()["max_date"]
    cutoff_value = df.select(F.date_sub(F.lit(max_date), CHURN_WINDOW_DAYS).alias("cutoff_date")).first()["cutoff_date"]
    
    historical_df = df.filter(F.col("TransactionDate") <= F.lit(cutoff_value))
    future_df = df.filter(F.col("TransactionDate") > F.lit(cutoff_value))
    
    customer_features = historical_df.groupBy("Customer ID").agg(
        F.sum("Line Total USD").alias("CustomerTotalRevenue"),
        F.countDistinct("Invoice ID").alias("CustomerPurchaseCount"),
        F.sum("Quantity").alias("CustomerTotalQuantity"),
        F.sum("Profit USD").alias("CustomerTotalProfit"),
        F.avg("Profit USD").alias("CustomerAverageProfit"),
        F.avg("Discount").alias("CustomerAverageDiscount"),
        F.avg(F.when(F.col("Promo Active") == True, 1.0).otherwise(0.0)).alias("CustomerPromoPurchaseRate"),
        F.avg(F.when(F.col("Is Weekend") == True, 1.0).otherwise(0.0)).alias("CustomerWeekendPurchaseRate"),
        F.max("TransactionDate").alias("CustomerLastPurchaseDate")
    )
    
    customer_features = customer_features.withColumn("CustomerRecencyDays", F.datediff(F.lit(cutoff_value), F.col("CustomerLastPurchaseDate")))
    customer_features = customer_features.withColumn("CustomerAverageOrderValue", F.when(F.col("CustomerPurchaseCount") > 0, F.col("CustomerTotalRevenue") / F.col("CustomerPurchaseCount")).otherwise(0.0))
    customer_features = customer_features.withColumn("CustomerProfitMargin", F.when(F.col("CustomerTotalRevenue") > 0, F.col("CustomerTotalProfit") / F.col("CustomerTotalRevenue")).otherwise(0.0))
    
    future_customers = future_df.select("Customer ID").distinct().withColumn("FuturePurchase", F.lit(1))
    ml_df = customer_features.join(future_customers, on="Customer ID", how="left")
    ml_df = ml_df.withColumn("ChurnLabel", F.when(F.col("FuturePurchase").isNull(), 1.0).otherwise(0.0)).drop("FuturePurchase")
    
    return ml_df.fillna({
        "CustomerTotalRevenue": 0.0, "CustomerPurchaseCount": 0, "CustomerTotalQuantity": 0,
        "CustomerTotalProfit": 0.0, "CustomerAverageProfit": 0.0, "CustomerAverageDiscount": 0.0,
        "CustomerPromoPurchaseRate": 0.0, "CustomerWeekendPurchaseRate": 0.0, "CustomerRecencyDays": 0,
        "CustomerAverageOrderValue": 0.0, "CustomerProfitMargin": 0.0
    })

def create_pipeline(classifier, use_scaler=False):
    feature_columns = get_feature_columns()
    if use_scaler:
        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features_raw", handleInvalid="keep")
        scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=False)
        return Pipeline(stages=[assembler, scaler, classifier])
    else:
        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features", handleInvalid="keep")
        return Pipeline(stages=[assembler, classifier])

def evaluate_model(predictions, model_name):
    roc_auc = BinaryClassificationEvaluator(labelCol="ChurnLabel", rawPredictionCol="rawPrediction", metricName="areaUnderROC").evaluate(predictions)
    f1_score = MulticlassClassificationEvaluator(labelCol="ChurnLabel", predictionCol="prediction", metricName="f1").evaluate(predictions)
    precision = MulticlassClassificationEvaluator(labelCol="ChurnLabel", predictionCol="prediction", metricName="weightedPrecision").evaluate(predictions)
    recall = MulticlassClassificationEvaluator(labelCol="ChurnLabel", predictionCol="prediction", metricName="weightedRecall").evaluate(predictions)
    
    churn_metrics = predictions.select(F.col("ChurnLabel").cast("double"), F.col("prediction").cast("double"))
    tp = churn_metrics.filter((F.col("ChurnLabel") == 1.0) & (F.col("prediction") == 1.0)).count()
    fp = churn_metrics.filter((F.col("ChurnLabel") == 0.0) & (F.col("prediction") == 1.0)).count()
    fn = churn_metrics.filter((F.col("ChurnLabel") == 1.0) & (F.col("prediction") == 0.0)).count()
    
    churn_precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    churn_recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    churn_f1 = (2 * churn_precision * churn_recall / (churn_precision + churn_recall)) if (churn_precision + churn_recall) > 0 else 0.0
    
    return {"Model": model_name, "ROC_AUC": roc_auc, "F1": f1_score, "Precision": precision, "Recall": recall, "ChurnPrecision": churn_precision, "ChurnRecall": churn_recall, "ChurnF1": churn_f1}

def train_models(train_df, test_df):
    models = {
        "LogisticRegression": {"classifier": LogisticRegression(featuresCol="features", labelCol="ChurnLabel", maxIter=50, regParam=0.01), "use_scaler": True},
        "DecisionTree": {"classifier": DecisionTreeClassifier(featuresCol="features", labelCol="ChurnLabel", maxDepth=8, seed=RANDOM_SEED), "use_scaler": False},
        "RandomForest": {"classifier": RandomForestClassifier(featuresCol="features", labelCol="ChurnLabel", numTrees=20, maxDepth=8, seed=RANDOM_SEED, featureSubsetStrategy="auto"), "use_scaler": False}
    }
    results, best_score, best_model_name, best_pipeline_model = [], -1.0, None, None
    
    for model_name, config in models.items():
        pipeline = create_pipeline(config["classifier"], use_scaler=config["use_scaler"])
        pipeline_model = pipeline.fit(train_df)
        predictions = pipeline_model.transform(test_df).persist(StorageLevel.MEMORY_AND_DISK)
        predictions.count()
        metrics = evaluate_model(predictions, model_name)
        results.append(metrics)
        
        if metrics["ChurnF1"] > best_score:
            best_score, best_model_name, best_pipeline_model = metrics["ChurnF1"], model_name, pipeline_model
        predictions.unpersist()
        
    return results, best_model_name, best_pipeline_model

def run_churn_prediction(spark):
    df = spark.read.parquet(FACT_PATH)
    ml_df = prepare_churn_dataset(df).persist(StorageLevel.MEMORY_AND_DISK)
    ml_df.count()
    
    train_df, test_df = ml_df.randomSplit([TRAIN_RATIO, 1 - TRAIN_RATIO], seed=RANDOM_SEED)
    train_df, test_df = train_df.persist(StorageLevel.MEMORY_AND_DISK), test_df.persist(StorageLevel.MEMORY_AND_DISK)
    train_count, test_count = train_df.count(), test_df.count()
    
    results, best_model_name, best_pipeline_model = train_models(train_df, test_df)
    
    final_predictions = best_pipeline_model.transform(ml_df) \
        .withColumn("probability_array", vector_to_array(F.col("probability"))) \
        .withColumn("ChurnProbability", F.col("probability_array")[1]) \
        .withColumnRenamed("prediction", "PredictedChurn") \
        .withColumn("RiskLevel", F.when(F.col("ChurnProbability") >= 0.80, F.lit("HIGH")).when(F.col("ChurnProbability") >= 0.50, F.lit("MEDIUM")).otherwise(F.lit("LOW"))) \
        .select("Customer ID", "ChurnLabel", "ChurnProbability", "PredictedChurn", "RiskLevel")
    
    final_predictions.write.mode("overwrite").parquet(PREDICTIONS_PATH)
    best_pipeline_model.write().overwrite().save(MODEL_PATH)
    
    train_df.unpersist()
    test_df.unpersist()
    ml_df.unpersist()

if __name__ == "__main__":
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        run_churn_prediction(spark)
    finally:
        spark.stop()