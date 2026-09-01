"""
Supervised Machine Learning Pipeline
Customer Churn Prediction

Global Fashion Retail Big Data Pipeline

Optimized Spark version.

Main improvements:

1. Historical and future windows prevent target leakage.
2. Expensive datasets are persisted to avoid recomputation.
3. StandardScaler is only used where appropriate.
4. Tree models do not unnecessarily scale features.
5. Train/Test datasets are cached.
6. Predictions are unpersisted after evaluation.
7. Churn-specific metrics are calculated.
8. Spark memory pressure is reduced.
"""

import os

from pyspark import StorageLevel

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark.ml import Pipeline

from pyspark.ml.feature import (
    VectorAssembler,
    StandardScaler
)

from pyspark.ml.functions import vector_to_array

from pyspark.ml.classification import (
    LogisticRegression,
    DecisionTreeClassifier,
    RandomForestClassifier
)

from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator
)


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


PREDICTIONS_PATH = (
    f"{HDFS_URI}/data/processed/churn_predictions"
)


MODEL_PATH = (
    f"{HDFS_URI}/models/customer_churn_model"
)


# ============================================================
# ML Configuration
# ============================================================

RANDOM_SEED = 42

TRAIN_RATIO = 0.8

CHURN_WINDOW_DAYS = 90


# ============================================================
# Spark Session
# ============================================================

def get_spark():

    return (

        SparkSession.builder

        .appName(
            "FashionRetailCustomerChurnPrediction"
        )

        .config(
            "spark.sql.shuffle.partitions",
            "16"
        )

        .config(
            "spark.sql.adaptive.enabled",
            "true"
        )

        .config(
            "spark.sql.adaptive.coalescePartitions.enabled",
            "true"
        )

        .config(
            "spark.sql.parquet.enableVectorizedReader",
            "true"
        )

        .getOrCreate()

    )


# ============================================================
# Feature Columns
# ============================================================

def get_feature_columns():

    return [

        "CustomerTotalRevenue",

        "CustomerPurchaseCount",

        "CustomerTotalQuantity",

        "CustomerAverageOrderValue",

        "CustomerTotalProfit",

        "CustomerAverageProfit",

        "CustomerAverageDiscount",

        "CustomerPromoPurchaseRate",

        "CustomerWeekendPurchaseRate",

        "CustomerRecencyDays",

        "CustomerProfitMargin"

    ]


# ============================================================
# Dataset Preparation
# ============================================================

def prepare_churn_dataset(df):

    """
    Creates a supervised ML dataset.

    Historical data:
        Used to create customer features.

    Future window:
        Used ONLY to create the churn label.

    ChurnLabel:

        1 = No purchase in future window
        0 = Purchased in future window
    """


    # ========================================================
    # Convert Date
    # ========================================================

    df = (

        df

        .withColumn(
            "TransactionDate",
            F.to_date("Date")
        )

        .filter(
            F.col("TransactionDate").isNotNull()
        )

    )


    # ========================================================
    # Dataset Maximum Date
    # ========================================================

    max_date = (

        df

        .agg(
            F.max("TransactionDate")
            .alias("max_date")
        )

        .first()["max_date"]

    )


    print(
        f"\n[churn_prediction] "
        f"Dataset maximum date: {max_date}"
    )


    # ========================================================
    # Cutoff Date
    # ========================================================

    cutoff_value = (

        df

        .select(
            F.date_sub(
                F.lit(max_date),
                CHURN_WINDOW_DAYS
            ).alias("cutoff_date")
        )

        .first()["cutoff_date"]

    )


    print(
        f"[churn_prediction] "
        f"Dynamic cutoff date: {cutoff_value}"
    )

    print(
        f"[churn_prediction] "
        f"Churn observation window: "
        f"{CHURN_WINDOW_DAYS} days"
    )


    # ========================================================
    # Historical Data
    # ========================================================

    historical_df = (

        df

        .filter(
            F.col("TransactionDate")
            <= F.lit(cutoff_value)
        )

    )


    # ========================================================
    # Future Data
    # ========================================================

    future_df = (

        df

        .filter(
            F.col("TransactionDate")
            > F.lit(cutoff_value)
        )

    )


    # ========================================================
    # Historical Customer Features
    # ========================================================

    print(
        "\n[churn_prediction] "
        "Building historical customer features..."
    )


    customer_features = (

        historical_df

        .groupBy("Customer ID")

        .agg(

            F.sum(
                "Line Total USD"
            ).alias(
                "CustomerTotalRevenue"
            ),

            F.countDistinct(
                "Invoice ID"
            ).alias(
                "CustomerPurchaseCount"
            ),

            F.sum(
                "Quantity"
            ).alias(
                "CustomerTotalQuantity"
            ),

            F.sum(
                "Profit USD"
            ).alias(
                "CustomerTotalProfit"
            ),

            F.avg(
                "Profit USD"
            ).alias(
                "CustomerAverageProfit"
            ),

            F.avg(
                "Discount"
            ).alias(
                "CustomerAverageDiscount"
            ),

            F.avg(

                F.when(
                    F.col("Promo Active") == True,
                    F.lit(1.0)
                )

                .otherwise(
                    F.lit(0.0)
                )

            ).alias(
                "CustomerPromoPurchaseRate"
            ),

            F.avg(

                F.when(
                    F.col("Is Weekend") == True,
                    F.lit(1.0)
                )

                .otherwise(
                    F.lit(0.0)
                )

            ).alias(
                "CustomerWeekendPurchaseRate"
            ),

            F.max(
                "TransactionDate"
            ).alias(
                "CustomerLastPurchaseDate"
            )

        )

    )


    # ========================================================
    # Recency
    # ========================================================

    customer_features = (

        customer_features

        .withColumn(

            "CustomerRecencyDays",

            F.datediff(

                F.lit(cutoff_value),

                F.col(
                    "CustomerLastPurchaseDate"
                )

            )

        )

    )


    # ========================================================
    # Average Order Value
    # ========================================================

    customer_features = (

        customer_features

        .withColumn(

            "CustomerAverageOrderValue",

            F.when(

                F.col(
                    "CustomerPurchaseCount"
                ) > 0,

                F.col(
                    "CustomerTotalRevenue"
                )

                /

                F.col(
                    "CustomerPurchaseCount"
                )

            )

            .otherwise(
                F.lit(0.0)
            )

        )

    )


    # ========================================================
    # Profit Margin
    # ========================================================

    customer_features = (

        customer_features

        .withColumn(

            "CustomerProfitMargin",

            F.when(

                F.col(
                    "CustomerTotalRevenue"
                ) > 0,

                F.col(
                    "CustomerTotalProfit"
                )

                /

                F.col(
                    "CustomerTotalRevenue"
                )

            )

            .otherwise(
                F.lit(0.0)
            )

        )

    )


    # ========================================================
    # Future Customer Activity
    # ========================================================

    print(
        "[churn_prediction] "
        "Creating churn labels..."
    )


    future_customers = (

        future_df

        .select(
            "Customer ID"
        )

        .distinct()

        .withColumn(
            "FuturePurchase",
            F.lit(1)
        )

    )


    # ========================================================
    # Join Features + Future Activity
    # ========================================================

    ml_df = (

        customer_features

        .join(

            future_customers,

            on="Customer ID",

            how="left"

        )

    )


    # ========================================================
    # Churn Label
    # ========================================================

    ml_df = (

        ml_df

        .withColumn(

            "ChurnLabel",

            F.when(

                F.col(
                    "FuturePurchase"
                ).isNull(),

                F.lit(1.0)

            )

            .otherwise(
                F.lit(0.0)
            )

        )

        .drop(
            "FuturePurchase"
        )

    )


    # ========================================================
    # Handle Null Values
    # ========================================================

    ml_df = ml_df.fillna({

        "CustomerTotalRevenue": 0.0,

        "CustomerPurchaseCount": 0,

        "CustomerTotalQuantity": 0,

        "CustomerTotalProfit": 0.0,

        "CustomerAverageProfit": 0.0,

        "CustomerAverageDiscount": 0.0,

        "CustomerPromoPurchaseRate": 0.0,

        "CustomerWeekendPurchaseRate": 0.0,

        "CustomerRecencyDays": 0,

        "CustomerAverageOrderValue": 0.0,

        "CustomerProfitMargin": 0.0

    })


    return ml_df


# ============================================================
# Class Distribution
# ============================================================

def show_class_distribution(df):

    print(
        "\n[churn_prediction] "
        "Churn distribution:"
    )


    (

        df

        .groupBy("ChurnLabel")

        .count()

        .orderBy("ChurnLabel")

        .show(
            truncate=False
        )

    )


# ============================================================
# Pipeline Creation
# ============================================================

def create_pipeline(
    classifier,
    use_scaler=False
):


    feature_columns = (
        get_feature_columns()
    )


    # ========================================================
    # Logistic Regression Pipeline
    # ========================================================

    if use_scaler:

        assembler = VectorAssembler(

            inputCols=feature_columns,

            outputCol="features_raw",

            handleInvalid="keep"

        )


        scaler = StandardScaler(

            inputCol="features_raw",

            outputCol="features",

            withStd=True,

            withMean=False

        )


        stages = [

            assembler,

            scaler,

            classifier

        ]


    # ========================================================
    # Tree Model Pipeline
    # ========================================================

    else:

        assembler = VectorAssembler(

            inputCols=feature_columns,

            outputCol="features",

            handleInvalid="keep"

        )


        stages = [

            assembler,

            classifier

        ]


    return Pipeline(
        stages=stages
    )


# ============================================================
# Model Evaluation
# ============================================================

def evaluate_model(predictions, model_name):


    # ========================================================
    # ROC-AUC
    # ========================================================

    roc_evaluator = BinaryClassificationEvaluator(

        labelCol="ChurnLabel",

        rawPredictionCol="rawPrediction",

        metricName="areaUnderROC"

    )


    roc_auc = roc_evaluator.evaluate(
        predictions
    )


    # ========================================================
    # Weighted F1
    # ========================================================

    f1_evaluator = MulticlassClassificationEvaluator(

        labelCol="ChurnLabel",

        predictionCol="prediction",

        metricName="f1"

    )


    f1_score = f1_evaluator.evaluate(
        predictions
    )


    # ========================================================
    # Weighted Precision
    # ========================================================

    precision_evaluator = MulticlassClassificationEvaluator(

        labelCol="ChurnLabel",

        predictionCol="prediction",

        metricName="weightedPrecision"

    )


    precision = precision_evaluator.evaluate(
        predictions
    )


    # ========================================================
    # Weighted Recall
    # ========================================================

    recall_evaluator = MulticlassClassificationEvaluator(

        labelCol="ChurnLabel",

        predictionCol="prediction",

        metricName="weightedRecall"

    )


    recall = recall_evaluator.evaluate(
        predictions
    )


    # ========================================================
    # Churn Class Metrics
    #
    # ChurnLabel = 1
    # ========================================================

    churn_metrics = (

        predictions

        .select(

            F.col("ChurnLabel").cast("double"),

            F.col("prediction").cast("double")

        )

    )


    # ========================================================
    # Calculate Confusion Matrix Values
    # ========================================================

    true_positive = churn_metrics.filter(

        (F.col("ChurnLabel") == 1.0) &

        (F.col("prediction") == 1.0)

    ).count()


    false_positive = churn_metrics.filter(

        (F.col("ChurnLabel") == 0.0) &

        (F.col("prediction") == 1.0)

    ).count()


    false_negative = churn_metrics.filter(

        (F.col("ChurnLabel") == 1.0) &

        (F.col("prediction") == 0.0)

    ).count()


    # ========================================================
    # Churn Precision
    # ========================================================

    churn_precision = (

        true_positive /

        (true_positive + false_positive)

        if (true_positive + false_positive) > 0

        else 0.0

    )


    # ========================================================
    # Churn Recall
    # ========================================================

    churn_recall = (

        true_positive /

        (true_positive + false_negative)

        if (true_positive + false_negative) > 0

        else 0.0

    )


    # ========================================================
    # Churn F1
    # ========================================================

    churn_f1 = (

        2 *

        (
            churn_precision *
            churn_recall
        )

        /

        (
            churn_precision +
            churn_recall
        )

        if (
            churn_precision +
            churn_recall
        ) > 0

        else 0.0

    )


    return {

        "Model": model_name,

        "ROC_AUC": roc_auc,

        "F1": f1_score,

        "Precision": precision,

        "Recall": recall,

        "ChurnPrecision": churn_precision,

        "ChurnRecall": churn_recall,

        "ChurnF1": churn_f1

    }


# ============================================================
# Train Models
# ============================================================

def train_models(
    train_df,
    test_df
):


    models = {


        "LogisticRegression": {

            "classifier":

            LogisticRegression(

                featuresCol="features",

                labelCol="ChurnLabel",

                maxIter=50,

                regParam=0.01

            ),

            "use_scaler": True

        },


        "DecisionTree": {

            "classifier":

            DecisionTreeClassifier(

                featuresCol="features",

                labelCol="ChurnLabel",

                maxDepth=8,

                seed=RANDOM_SEED

            ),

            "use_scaler": False

        },


        "RandomForest": {

            "classifier":

            RandomForestClassifier(

                featuresCol="features",

                labelCol="ChurnLabel",

                numTrees=20,

                maxDepth=8,

                seed=RANDOM_SEED,

                featureSubsetStrategy="auto"

            ),

            "use_scaler": False

        }

    }


    results = []


    # ========================================================
    # Best Model Tracking
    #
    # Primary metric: Churn F1
    # ========================================================

    best_score = -1.0

    best_model_name = None

    best_pipeline_model = None


    # ========================================================
    # Train Each Model
    # ========================================================

    for model_name, model_config in models.items():


        print(

            f"\n[churn_prediction] "
            f"Training {model_name}..."

        )


        classifier = (
            model_config["classifier"]
        )


        use_scaler = (
            model_config["use_scaler"]
        )


        pipeline = create_pipeline(

            classifier,

            use_scaler=use_scaler

        )


        # ====================================================
        # Train
        # ====================================================

        pipeline_model = pipeline.fit(
            train_df
        )


        # ====================================================
        # Predict
        # ====================================================

        predictions = (

            pipeline_model

            .transform(test_df)

            .persist(
                StorageLevel.MEMORY_AND_DISK
            )

        )


        # Materialize cache
        predictions.count()


        # ====================================================
        # Evaluate
        # ====================================================

        metrics = evaluate_model(

            predictions,

            model_name

        )


        results.append(
            metrics
        )


        # ====================================================
        # Print Results
        # ====================================================

        print(

            f"\n[churn_prediction] "
            f"{model_name} results:"

        )


        print(
            f"ROC-AUC: "
            f"{metrics['ROC_AUC']:.4f}"
        )


        print(
            f"F1 Score: "
            f"{metrics['F1']:.4f}"
        )


        print(
            f"Precision: "
            f"{metrics['Precision']:.4f}"
        )


        print(
            f"Recall: "
            f"{metrics['Recall']:.4f}"
        )


        print(
            f"\nChurn Precision: "
            f"{metrics['ChurnPrecision']:.4f}"
        )


        print(
            f"Churn Recall: "
            f"{metrics['ChurnRecall']:.4f}"
        )


        print(
            f"Churn F1: "
            f"{metrics['ChurnF1']:.4f}"
        )


        # ====================================================
        # Select Best Model
        #
        # Primary Metric:
        # Churn F1
        # ====================================================

        current_score = metrics["ChurnF1"]


        if current_score > best_score:

            best_score = current_score

            best_model_name = model_name

            best_pipeline_model = pipeline_model


        # ====================================================
        # Release Prediction Memory
        # ====================================================

        predictions.unpersist()


    return (

        results,

        best_model_name,

        best_pipeline_model

    )


# ============================================================
# Main Pipeline
# ============================================================

def run_churn_prediction(spark):


    # ========================================================
    # Load Fact Table
    # ========================================================

    print(

        f"\n[churn_prediction] "
        f"Loading fact table from:\n"
        f"{FACT_PATH}\n"

    )


    df = (

        spark.read

        .parquet(
            FACT_PATH
        )

    )


    print(
        "[churn_prediction] "
        "Fact table loaded successfully."
    )


    # ========================================================
    # Prepare ML Dataset
    # ========================================================

    print(

        "\n[churn_prediction] "
        "Preparing supervised ML dataset..."

    )


    ml_df = (

        prepare_churn_dataset(df)

        .persist(
            StorageLevel.MEMORY_AND_DISK
        )

    )


    # Materialize dataset
    customer_count = ml_df.count()


    print(

        f"\n[churn_prediction] "
        f"ML customer rows: "
        f"{customer_count:,}"

    )


    # ========================================================
    # Class Distribution
    # ========================================================

    show_class_distribution(
        ml_df
    )


    # ========================================================
    # Train/Test Split
    # ========================================================

    print(

        "\n[churn_prediction] "
        "Creating Train/Test split..."

    )


    train_df, test_df = (

        ml_df

        .randomSplit(

            [

                TRAIN_RATIO,

                1 - TRAIN_RATIO

            ],

            seed=RANDOM_SEED

        )

    )


    # ========================================================
    # Persist Train/Test Data
    # ========================================================

    train_df = (

        train_df

        .persist(
            StorageLevel.MEMORY_AND_DISK
        )

    )


    test_df = (

        test_df

        .persist(
            StorageLevel.MEMORY_AND_DISK
        )

    )


    train_count = train_df.count()

    test_count = test_df.count()


    print(

        f"[churn_prediction] "
        f"Training rows: "
        f"{train_count:,}"

    )


    print(

        f"[churn_prediction] "
        f"Testing rows: "
        f"{test_count:,}"

    )


    # ========================================================
    # Train Models
    # ========================================================

    print(

        "\n[churn_prediction] "
        "Training supervised ML models..."

    )


    (

        results,

        best_model_name,

        best_pipeline_model

    ) = train_models(

        train_df,

        test_df

    )


    # ========================================================
    # Model Comparison
    # ========================================================

    print(
        "\n"
        "===================================================="
    )

    print(
        "[churn_prediction] MODEL COMPARISON"
    )

    print(
        "===================================================="
    )


    for result in results:


        print(
            f"\nModel: "
            f"{result['Model']}"
        )


        print(
            f"ROC-AUC: "
            f"{result['ROC_AUC']:.4f}"
        )


        print(
            f"F1: "
            f"{result['F1']:.4f}"
        )


        print(
            f"Precision: "
            f"{result['Precision']:.4f}"
        )


        print(
            f"Recall: "
            f"{result['Recall']:.4f}"
        )


        print(
            f"Churn Precision: "
            f"{result['ChurnPrecision']:.4f}"
        )


        print(
            f"Churn Recall: "
            f"{result['ChurnRecall']:.4f}"
        )


        print(
            f"Churn F1: "
            f"{result['ChurnF1']:.4f}"
        )


    # ========================================================
    # Best Model
    # ========================================================

    print(
        "\n"
        "===================================================="
    )

    print(
        "[churn_prediction] BEST MODEL"
    )

    print(
        "===================================================="
    )


    print(
        f"\nBest Model: "
        f"{best_model_name}"
    )


    # ========================================================
    # Generate Final Predictions
    # ========================================================

    print(

        "\n[churn_prediction] "
        "Generating customer predictions..."

    )


    final_predictions = (

        best_pipeline_model

        .transform(ml_df)

        # Convert Vector probability to Array
        .withColumn(

            "probability_array",

            vector_to_array(
                F.col("probability")
            )

        )

        # Extract probability of ChurnLabel = 1
        .withColumn(

            "ChurnProbability",

            F.col(
                "probability_array"
            )[1]

        )

        # Rename prediction
        .withColumnRenamed(

            "prediction",

            "PredictedChurn"

        )

        # Create Risk Level
        .withColumn(

            "RiskLevel",

            F.when(

                F.col(
                    "ChurnProbability"
                ) >= 0.80,

                F.lit("HIGH")

            )

            .when(

                F.col(
                    "ChurnProbability"
                ) >= 0.50,

                F.lit("MEDIUM")

            )

            .otherwise(

                F.lit("LOW")

            )

        )

        # Final business-friendly columns
        .select(

            "Customer ID",

            "ChurnLabel",

            "ChurnProbability",

            "PredictedChurn",

            "RiskLevel"

        )

    )


    # ========================================================
    # Write Predictions
    # ========================================================

    print(

        f"\n[churn_prediction] "
        f"Writing predictions to:\n"
        f"{PREDICTIONS_PATH}"

    )


    (

        final_predictions

        .write

        .mode("overwrite")

        .parquet(
            PREDICTIONS_PATH
        )

    )


    # ========================================================
    # Save Model
    # ========================================================

    print(

        f"\n[churn_prediction] "
        f"Saving complete ML pipeline to:\n"
        f"{MODEL_PATH}"

    )


    (

        best_pipeline_model

        .write()

        .overwrite()

        .save(
            MODEL_PATH
        )

    )


    # ========================================================
    # Release Spark Memory
    # ========================================================

    train_df.unpersist()

    test_df.unpersist()

    ml_df.unpersist()


    # ========================================================
    # Completed
    # ========================================================

    print(

        "\n"
        "===================================================="

    )

    print(

        "[churn_prediction] "
        "Customer churn prediction completed successfully."

    )

    print(

        "====================================================\n"

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

        run_churn_prediction(
            spark
        )


    finally:

        spark.stop()