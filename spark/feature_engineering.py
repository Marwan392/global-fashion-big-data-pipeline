"""
Multi-Model Training Pipeline for Global Fashion Retail Pipeline.
Trains Classification models (Churn Prediction) and a Linear Regression model (Spend Forecasting).
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression, DecisionTreeClassifier
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator, RegressionEvaluator

# Initialize HDFS URI and feature store path
HDFS_URI = os.environ.get("HDFS_URI", "hdfs://namenode:9000")
FEATURE_PATH = f"{HDFS_URI}/data/processed/engineered_features"

def get_spark():
    """Initializes and returns the Spark session."""
    return SparkSession.builder \
        .appName("FashionRetailMultiModelTraining") \
        .getOrCreate()

if __name__ == "__main__":
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Load the preprocessed feature store from HDFS
    print(f"Loading engineered features from {FEATURE_PATH} ...", flush=True)
    df = spark.read.parquet(FEATURE_PATH)

    # ==========================================
    # PART 1: CLASSIFICATION MODELS (Customer Churn)
    # ==========================================
    print("\n--- Running Classification Pipeline (Customer Churn) ---", flush=True)
    
    # Define binary target label: 1 if customer hasn't purchased in over 90 days (Churned), else 0
    df_class = df.withColumn("Label", F.when(F.col("CustomerRecencyDays") > 90, 1.0).otherwise(0.0))

    # Define feature set used for modeling
    feature_columns = [
        "CustomerTotalSpend", "CustomerPurchaseCount", "CustomerAvgProfit",
        "CustomerTotalQuantity", "CustomerRecencyDays", "DiscountImpact", 
        "ProfitMargin", "StoreCountryIndex", "ColorIndex", "AgeGroupIndex", 
        "CategoryIndex", "Promo Active", "Is Weekend"
    ]

    # Drop missing values for the selected features and label
    class_data = df_class.select(feature_columns + ["Label"]).na.drop()
    
    # Assemble individual feature columns into a single ML feature vector
    assembler_class = VectorAssembler(inputCols=feature_columns, outputCol="features")
    dataset_class = assembler_class.transform(class_data).select("features", "Label")

    # Split dataset into training (80%) and testing (20%) sets
    train_c, test_c = dataset_class.randomSplit([0.8, 0.2], seed=42)

    # Define the dictionary of classification models to evaluate
    classifiers = {
        "Logistic Regression": LogisticRegression(featuresCol="features", labelCol="Label"),
        "Decision Tree": DecisionTreeClassifier(featuresCol="features", labelCol="Label"),
        "Random Forest": RandomForestClassifier(featuresCol="features", labelCol="Label", numTrees=50)
    }

    # Set up evaluators for ROC-AUC and Accuracy metrics
    evaluator_roc = BinaryClassificationEvaluator(labelCol="Label", metricName="areaUnderROC")
    evaluator_acc = MulticlassClassificationEvaluator(labelCol="Label", metricName="accuracy")

    print("\n================ CLASSIFICATION LEADERBOARD ================")
    best_score = 0.0
    best_model_name = ""
    best_model_obj = None

    # Iteratively train and evaluate each classifier
    for name, algo in classifiers.items():
        print(f"Training {name}...", flush=True)
        model = algo.fit(train_c)
        predictions = model.transform(test_c)
        
        roc_auc = evaluator_roc.evaluate(predictions)
        accuracy = evaluator_acc.evaluate(predictions)
        
        print(f"-> {name} | ROC-AUC: {roc_auc:.4f} | Accuracy: {accuracy:.4f}")

        # Track the best performing model based on ROC-AUC score
        if roc_auc > best_score:
            best_score = roc_auc
            best_model_name = name
            best_model_obj = model

    print("===============================================================")
    print(f"🏆 Best Classification Model: {best_model_name} (ROC-AUC: {best_score:.4f})")
    
    # Save the winning classification model to HDFS for web application consumption
    clf_output_path = f"{HDFS_URI}/data/models/best_churn_model"
    print(f"Saving best classification model to {clf_output_path} ...", flush=True)
    best_model_obj.write().overwrite().save(clf_output_path)


    # ==========================================
    # PART 2: LINEAR REGRESSION MODEL (Spend Forecasting)
    # ==========================================
    print("\n--- Running Linear Regression Pipeline (Spend Forecasting) ---", flush=True)

    # Define continuous target label for regression: Total customer spend amount
    df_reg = df.withColumn("Label", F.col("CustomerTotalSpend"))
    reg_data = df_reg.select(feature_columns + ["Label"]).na.drop()

    # Assemble features for regression task
    assembler_reg = VectorAssembler(inputCols=feature_columns, outputCol="features")
    dataset_reg = assembler_reg.transform(reg_data).select("features", "Label")

    # Split dataset into training (80%) and testing (20%) sets
    train_r, test_r = dataset_reg.randomSplit([0.8, 0.2], seed=42)

    # Train the Linear Regression model
    print("Training Linear Regression model...", flush=True)
    lr = LinearRegression(featuresCol="features", labelCol="Label")
    lr_model = lr.fit(train_r)

    # Evaluate regression performance using RMSE and R-Squared
    preds_reg = lr_model.transform(test_r)
    eval_rmse = RegressionEvaluator(labelCol="Label", predictionCol="prediction", metricName="rmse")
    eval_r2 = RegressionEvaluator(labelCol="Label", predictionCol="prediction", metricName="r2")

    rmse = eval_rmse.evaluate(preds_reg)
    r2 = eval_r2.evaluate(preds_reg)

    print("\n================ LINEAR REGRESSION RESULTS ================")
    print(f"-> Linear Regression | RMSE: {rmse:.4f} | R-Squared (R2): {r2:.4f}")
    print("============================================================")

    # Save the trained linear regression model to HDFS
    reg_output_path = f"{HDFS_URI}/data/models/spend_linear_regression"
    print(f"Saving Linear Regression model to {reg_output_path} ...", flush=True)
    lr_model.write().overwrite().save(reg_output_path)

    print("\n🎉 All classification and regression models trained successfully!")
    spark.stop()