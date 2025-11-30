import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, month, year, from_unixtime, unix_timestamp,
    avg, stddev, min as spark_min, max as spark_max, count
)
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import requests

# ========================================================================
# CONFIGURATION
# ========================================================================

# ClickHouse Configuration
CLICKHOUSE_HOST = 'clickhouse'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DB = 'weather_analytics'

# HDFS Paths
HDFS_BASE = "hdfs://namenode:9000"
WEATHER_PATH = f"{HDFS_BASE}/user/data/ml_training_data/"  # get data from hdfs
MODEL_PATH = f"{HDFS_BASE}/user/models/et_prediction_model"

# Local Model Path (to save outside container)
LOCAL_MODEL_PATH = "/tmp/et_prediction_model"  # Will be copied to host

# Spark Configuration
SPARK_APP_NAME = "ET Prediction - Model Training"
SPARK_MASTER = "spark://spark-master:7077"

# ML Configuration
TRAIN_TEST_SPLIT = 0.8  # 80% training, 20% testing
RANDOM_SEED = 42
TARGET_MONTH = 5  # May

# Feature columns (with units as they appear in CSV)
FEATURE_COLS = [
    'precipitation_hours (h)',
    'sunshine_duration (s)',
    'wind_speed_10m_max (km/h)'
]
TARGET_COL = 'et0_fao_evapotranspiration (mm)'

# ========================================================================
# CLICKHOUSE FUNCTIONS
# ========================================================================

def execute_clickhouse_query(query, verbose=True):
    """Execute SQL query on ClickHouse via HTTP API"""
    url = f'http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}'
    params = {'database': CLICKHOUSE_DB}

    try:
        response = requests.post(url, params=params, data=query.encode('utf-8'), timeout=60)
        response.raise_for_status()
        if verbose:
            print(f"✓ ClickHouse query executed")
        return response.text
    except Exception as e:
        print(f"✗ ClickHouse error: {e}")
        return None

def create_ml_tables():
    """Create ClickHouse tables for ML results"""
    print("\n" + "="*80)
    print("CREATING CLICKHOUSE TABLES FOR ML RESULTS")
    print("="*80)

    # Table 1: Model Performance Metrics
    table1 = """
    CREATE TABLE IF NOT EXISTS ml_model_performance (
        model_name String,
        training_date DateTime DEFAULT now(),
        train_size UInt32,
        test_size UInt32,
        rmse Float64,
        r2 Float64,
        mae Float64,
        feature_1 String,
        feature_2 String,
        feature_3 String,
        coefficient_1 Float64,
        coefficient_2 Float64,
        coefficient_3 Float64,
        intercept Float64
    ) ENGINE = MergeTree()
    ORDER BY training_date
    """

    print("\n[1/2] Creating: ml_model_performance")
    execute_clickhouse_query(table1)

    # Table 2: Feature Statistics
    table2 = """
    CREATE TABLE IF NOT EXISTS ml_feature_statistics (
        month UInt8,
        feature_name String,
        mean_value Float64,
        std_dev Float64,
        min_value Float64,
        max_value Float64,
        sample_count UInt32,
        created_at DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY (month, feature_name)
    """

    print("[2/2] Creating: ml_feature_statistics")
    execute_clickhouse_query(table2)

    print("\n✓ ML tables created successfully\n")

def save_model_performance(metrics_dict):
    """Save model performance metrics to ClickHouse"""
    print("\n" + "="*80)
    print("SAVING MODEL PERFORMANCE TO CLICKHOUSE")
    print("="*80)

    insert_query = f"""
    INSERT INTO ml_model_performance (
        model_name, train_size, test_size, rmse, r2, mae,
        feature_1, feature_2, feature_3,
        coefficient_1, coefficient_2, coefficient_3, intercept
    ) VALUES (
        '{metrics_dict['model_name']}',
        {metrics_dict['train_size']},
        {metrics_dict['test_size']},
        {metrics_dict['rmse']},
        {metrics_dict['r2']},
        {metrics_dict['mae']},
        '{metrics_dict['features'][0]}',
        '{metrics_dict['features'][1]}',
        '{metrics_dict['features'][2]}',
        {metrics_dict['coefficients'][0]},
        {metrics_dict['coefficients'][1]},
        {metrics_dict['coefficients'][2]},
        {metrics_dict['intercept']}
    )
    """

    result = execute_clickhouse_query(insert_query, verbose=False)
    if result is not None:
        print("✓ Model performance saved to ClickHouse")
    else:
        print("✗ Failed to save model performance")

def save_feature_statistics(stats_df, month_num):
    """Save feature statistics to ClickHouse"""
    print("\nSaving feature statistics to ClickHouse...")

    stats_data = stats_df.collect()

    for row in stats_data:
        insert_query = f"""
        INSERT INTO ml_feature_statistics (
            month, feature_name, mean_value, std_dev, min_value, max_value, sample_count
        ) VALUES (
            {month_num},
            '{row['feature_name']}',
            {row['mean']},
            {row['stddev']},
            {row['min']},
            {row['max']},
            {row['count']}
        )
        """
        execute_clickhouse_query(insert_query, verbose=False)

    print(f"✓ Saved statistics for {len(stats_data)} features")

# ========================================================================
# MAIN TRAINING PIPELINE
# ========================================================================

def main():
    """Main ML training pipeline"""

    print("SPARK MLLIB - EVAPOTRANSPIRATION PREDICTION MODEL TRAINING")

    # ────────────────────────────────────────────────────────────────────
    # STEP 1: Initialize Spark Session
    # ────────────────────────────────────────────────────────────────────

    print("\n[STEP 1] Initializing Spark Session...")

    spark = SparkSession.builder \
        .appName(SPARK_APP_NAME) \
        .master(SPARK_MASTER) \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    print(f"✓ Spark Session initialized")
    print(f"  App ID: {spark.sparkContext.applicationId}")

    # ────────────────────────────────────────────────────────────────────
    # STEP 2: Create ClickHouse Tables
    # ────────────────────────────────────────────────────────────────────

    print("\n[STEP 2] Setting up ClickHouse tables...")
    create_ml_tables()

    # ────────────────────────────────────────────────────────────────────
    # STEP 3: Load and Prepare Data
    # ────────────────────────────────────────────────────────────────────

    print("\n[STEP 3] Loading weather data from HDFS...")
    print("="*80)

    # Load weather data
    print(f"Loading from: {WEATHER_PATH}")
    weather_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(WEATHER_PATH)

    total_records = weather_df.count()
    print(f"✓ Loaded {total_records:,} total weather records")

    # Parse dates and extract month
    print("\nParsing dates and extracting month...")
    weather_df = weather_df.withColumn(
        "date_parsed",
        from_unixtime(unix_timestamp(col("date"), "M/d/yyyy"))
    ).withColumn(
        "year", year(col("date_parsed"))
    ).withColumn(
        "month", month(col("date_parsed"))
    )

    # ────────────────────────────────────────────────────────────────────
    # STEP 4: Filter for May and Select Features
    # ────────────────────────────────────────────────────────────────────

    print("\n[STEP 4] Filtering data for May...")
    print("="*80)

    # Filter for May (month = 5)
    may_data = weather_df.filter(col("month") == TARGET_MONTH)
    may_count = may_data.count()
    print(f"✓ Filtered to {may_count:,} May records ({may_count/total_records*100:.1f}% of total)")

    # Select relevant columns and remove nulls
    print("\nSelecting features and removing nulls...")
    selected_cols = FEATURE_COLS + [TARGET_COL, 'year', 'month']
    may_selected = may_data.select(*selected_cols)

    # Convert sunshine_duration from seconds to hours
    print("Converting sunshine_duration from seconds to hours...")
    may_converted = may_selected.withColumn(
        'sunshine_duration (s)',
        col('sunshine_duration (s)') / 3600.0
    )

    # Remove nulls
    may_clean = may_converted.na.drop()

    clean_count = may_clean.count()
    print(f"✓ Clean dataset: {clean_count:,} records (removed {may_count - clean_count:,} null values)")

    # Show sample data
    print("\nSample data:")
    may_clean.select(*FEATURE_COLS, TARGET_COL).show(5, truncate=False)

    # ────────────────────────────────────────────────────────────────────
    # STEP 5: Feature Statistics & Analysis
    # ────────────────────────────────────────────────────────────────────

    print("\n[STEP 5] Analyzing feature statistics...")
    print("="*80)

    # Calculate statistics for each feature
    stats_list = []
    for feature in FEATURE_COLS + [TARGET_COL]:
        stats = may_clean.agg(
            avg(col(feature)).alias('mean'),
            stddev(col(feature)).alias('stddev'),
            spark_min(col(feature)).alias('min'),
            spark_max(col(feature)).alias('max'),
            count(col(feature)).alias('count')
        ).collect()[0]

        stats_list.append({
            'feature_name': feature,
            'mean': float(stats['mean']),
            'stddev': float(stats['stddev']),
            'min': float(stats['min']),
            'max': float(stats['max']),
            'count': int(stats['count'])
        })

        print(f"\n{feature}:")
        print(f"  Mean: {stats['mean']:.2f}")
        print(f"  Std Dev: {stats['stddev']:.2f}")
        print(f"  Min: {stats['min']:.2f}")
        print(f"  Max: {stats['max']:.2f}")

    # Convert to DataFrame and save to ClickHouse
    stats_df = spark.createDataFrame(stats_list)
    save_feature_statistics(stats_df, TARGET_MONTH)

    # ────────────────────────────────────────────────────────────────────
    # STEP 6: Feature Engineering
    # ────────────────────────────────────────────────────────────────────

    print("\n[STEP 6] Feature Engineering...")
    print("="*80)

    # Assemble features into a vector
    print("\nAssembling features into vector...")
    assembler = VectorAssembler(
        inputCols=FEATURE_COLS,
        outputCol="features_raw"
    )

    # Standardize features (important for Linear Regression)
    print("Adding feature scaling...")
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withStd=True,
        withMean=True
    )

    print(f"✓ Feature pipeline created:")
    print(f"  Input features: {FEATURE_COLS}")
    print(f"  Output: Scaled feature vector")

    # ────────────────────────────────────────────────────────────────────
    # STEP 7: Train/Test Split
    # ────────────────────────────────────────────────────────────────────

    print("\n[STEP 7] Splitting data into train/test sets...")
    print("="*80)

    # Split data (80% train, 20% test)
    train_data, test_data = may_clean.randomSplit(
        [TRAIN_TEST_SPLIT, 1 - TRAIN_TEST_SPLIT],
        seed=RANDOM_SEED
    )

    train_count = train_data.count()
    test_count = test_data.count()

    print(f"✓ Data split completed:")
    print(f"  Training set: {train_count:,} records ({train_count/clean_count*100:.1f}%)")
    print(f"  Test set: {test_count:,} records ({test_count/clean_count*100:.1f}%)")

    # ────────────────────────────────────────────────────────────────────
    # STEP 8: Train Linear Regression Model
    # ────────────────────────────────────────────────────────────────────

    print("\n[STEP 8] Training Linear Regression Model...")
    print("="*80)

    # Create Linear Regression model
    lr = LinearRegression(
        featuresCol="features",
        labelCol=TARGET_COL,
        maxIter=100,
        regParam=0.1,  # L2 regularization
        elasticNetParam=0.0  # Pure L2 (Ridge regression)
    )

    # Create ML Pipeline
    pipeline = Pipeline(stages=[assembler, scaler, lr])

    print("Training model...")
    print(f"  Algorithm: Linear Regression")
    print(f"  Max iterations: 100")
    print(f"  Regularization: L2 (Ridge)")

    # Train the model
    model = pipeline.fit(train_data)

    # Extract the trained Linear Regression model
    lr_model = model.stages[-1]

    print("\n✓ Model training completed!")
    print(f"\nModel Coefficients:")
    coefficients = lr_model.coefficients.toArray()
    for i, (feature, coef) in enumerate(zip(FEATURE_COLS, coefficients)):
        print(f"  {feature}: {coef:.6f}")
    print(f"  Intercept: {lr_model.intercept:.6f}")

    # Interpret coefficients
    print(f"\nModel Interpretation:")
    print(f"  ET = {lr_model.intercept:.3f} + ", end="")
    terms = []
    for feature, coef in zip(FEATURE_COLS, coefficients):
        sign = "+" if coef >= 0 else "-"
        terms.append(f"{sign} {abs(coef):.3f}×{feature}")
    print(" ".join(terms))

    # ────────────────────────────────────────────────────────────────────
    # STEP 9: Model Evaluation on Test Set
    # ────────────────────────────────────────────────────────────────────

    print("\n[STEP 9] Evaluating Model Performance...")
    print("="*80)

    # Make predictions on test set
    predictions = model.transform(test_data)

    # Show sample predictions
    print("\nSample Predictions (first 10):")
    predictions.select(
        *FEATURE_COLS,
        col(TARGET_COL).alias("actual_ET"),
        col("prediction").alias("predicted_ET")
    ).show(10, truncate=False)

    # Calculate evaluation metrics
    evaluator_rmse = RegressionEvaluator(
        labelCol=TARGET_COL,
        predictionCol="prediction",
        metricName="rmse"
    )

    evaluator_r2 = RegressionEvaluator(
        labelCol=TARGET_COL,
        predictionCol="prediction",
        metricName="r2"
    )

    evaluator_mae = RegressionEvaluator(
        labelCol=TARGET_COL,
        predictionCol="prediction",
        metricName="mae"
    )

    rmse = evaluator_rmse.evaluate(predictions)
    r2 = evaluator_r2.evaluate(predictions)
    mae = evaluator_mae.evaluate(predictions)

    print("\n" + "="*80)
    print("MODEL PERFORMANCE METRICS")
    print("="*80)
    print(f"Root Mean Squared Error (RMSE): {rmse:.4f} mm")
    print(f"R² (Coefficient of Determination): {r2:.4f}")
    print(f"  → Model explains {r2*100:.2f}% of variance in ET")
    print(f"Mean Absolute Error (MAE): {mae:.4f} mm")
    print("="*80)

    # Interpretation
    print("\nPerformance Interpretation:")
    if r2 > 0.8:
        print(f"  ✓ Excellent fit! R² = {r2:.4f} (>0.8)")
    elif r2 > 0.6:
        print(f"  ✓ Good fit. R² = {r2:.4f} (>0.6)")
    elif r2 > 0.4:
        print(f"  ⚠ Moderate fit. R² = {r2:.4f}")
    else:
        print(f"  ✗ Poor fit. R² = {r2:.4f} (<0.4)")

    print(f"  Average prediction error: ±{mae:.2f} mm")

    # ────────────────────────────────────────────────────────────────────
    # STEP 10: Save Model to HDFS and Locally
    # ────────────────────────────────────────────────────────────────────

    print("\n[STEP 10] Saving model...")
    print("="*80)

    # Save to HDFS
    try:
        print(f"\n[1/2] Saving model to HDFS...")
        model.write().overwrite().save(MODEL_PATH)
        print(f"✓ Model saved to HDFS: {MODEL_PATH}")
    except Exception as e:
        print(f"✗ Error saving model to HDFS: {e}")

    # Save to local filesystem (for copying out of container)
    try:
        print(f"\n[2/2] Saving model to local filesystem...")
        import shutil
        import os

        # Remove old local model if exists
        if os.path.exists(LOCAL_MODEL_PATH):
            shutil.rmtree(LOCAL_MODEL_PATH)

        model.write().overwrite().save(f"file://{LOCAL_MODEL_PATH}")
        print(f"✓ Model saved locally: {LOCAL_MODEL_PATH}")
        print(f"  (This will be copied to src/spark_mllib/model)")
    except Exception as e:
        print(f"✗ Error saving model locally: {e}")

    # ────────────────────────────────────────────────────────────────────
    # STEP 11: Save Results to ClickHouse
    # ────────────────────────────────────────────────────────────────────

    print("\n[STEP 11] Saving results to ClickHouse...")
    print("="*80)

    metrics_dict = {
        'model_name': 'LinearRegression_ET_May',
        'train_size': train_count,
        'test_size': test_count,
        'rmse': rmse,
        'r2': r2,
        'mae': mae,
        'features': FEATURE_COLS,
        'coefficients': coefficients.tolist(),
        'intercept': float(lr_model.intercept)
    }

    save_model_performance(metrics_dict)

    # ────────────────────────────────────────────────────────────────────
    # STEP 12: Summary
    # ────────────────────────────────────────────────────────────────────

    print("\n" + "="*80)
    print("TRAINING COMPLETE!")
    print("="*80)

    print("\nSummary:")
    print(f"  ✓ Data loaded: {total_records:,} total records")
    print(f"  ✓ May data filtered: {clean_count:,} records")
    print(f"  ✓ Training set: {train_count:,} records")
    print(f"  ✓ Test set: {test_count:,} records")
    print(f"  ✓ Model trained: Linear Regression")
    print(f"  ✓ Performance: R² = {r2:.4f}, RMSE = {rmse:.4f} mm")
    print(f"  ✓ Model saved to HDFS: {MODEL_PATH}")
    print(f"  ✓ Model saved locally: {LOCAL_MODEL_PATH}")
    print(f"  ✓ Metrics saved to ClickHouse")

    print("\nNext Steps:")
    print("  → Run predict_et_model.py to make predictions for May 2026")
    print("  → Find optimal conditions where ET < 1.5mm")

    # Stop Spark session
    spark.stop()
    print("\n✓ Spark session stopped")

# ========================================================================
# ENTRY POINT
# ========================================================================

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n✗ Training interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
