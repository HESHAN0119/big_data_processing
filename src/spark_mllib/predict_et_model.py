#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Spark MLlib - Evapotranspiration Prediction for May 2026
=========================================================

Task 3: Load Trained Model and Make Predictions

PURPOSE:
    Load the trained Linear Regression model and predict evapotranspiration
    for different weather scenarios in May 2026.

OBJECTIVE:
    Find combinations of weather conditions that result in ET < 1.5mm:
    - precipitation_hours
    - sunshine_duration
    - wind_speed_10m_max

METHOD:
    - Load saved model from HDFS
    - Generate grid of possible weather conditions
    - Make predictions for each combination
    - Filter results where predicted ET < 1.5mm
    - Save recommendations to ClickHouse

USAGE:
    spark-submit --master spark://spark-master:7077 predict_et_model.py
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType
from pyspark.sql.functions import col, round as spark_round, lit
from pyspark.ml import PipelineModel
import requests
import itertools

# ========================================================================
# CONFIGURATION
# ========================================================================

# ClickHouse Configuration
CLICKHOUSE_HOST = 'clickhouse'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DB = 'weather_analytics'

# HDFS Paths
HDFS_BASE = "hdfs://namenode:9000"
MODEL_PATH = f"{HDFS_BASE}/user/models/et_prediction_model"

# Spark Configuration
SPARK_APP_NAME = "ET Prediction - May 2026 Forecast"
SPARK_MASTER = "spark://spark-master:7077"

# Prediction Configuration
TARGET_YEAR = 2026
TARGET_MONTH = 5  # May
ET_THRESHOLD = 1.5  # mm - Target ET threshold

# Feature ranges for grid search
# Based on historical May data statistics
PRECIPITATION_RANGE = list(range(0, 25, 2))  # 0-24 hours in steps of 2
SUNSHINE_RANGE = list(range(0, 12, 1))        # 0-11 hours in steps of 1
WIND_SPEED_RANGE = list(range(5, 26, 2))     # 5-25 km/h in steps of 2

FEATURE_COLS = [
    'precipitation_hours (h)',
    'sunshine_duration (s)',
    'wind_speed_10m_max (km/h)'
]

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

def create_prediction_table():
    """Create ClickHouse table for predictions"""
    print("\n" + "="*80)
    print("CREATING CLICKHOUSE TABLE FOR PREDICTIONS")
    print("="*80)

    table_sql = """
    CREATE TABLE IF NOT EXISTS ml_predictions_may_2026 (
        scenario_id UInt32,
        prediction_year UInt16,
        prediction_month UInt8,
        precipitation_hours Float64,
        sunshine_duration Float64,
        wind_speed_10m_max Float64,
        predicted_et Float64,
        meets_threshold UInt8,
        feasibility_score Float64,
        recommendation_rank UInt32,
        created_at DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY (meets_threshold DESC, predicted_et ASC)
    """

    print("\nCreating: ml_predictions_may_2026")
    execute_clickhouse_query(table_sql)
    print("✓ Prediction table created\n")

def save_predictions(predictions_df):
    """Save predictions to ClickHouse"""
    print("\n" + "="*80)
    print("SAVING PREDICTIONS TO CLICKHOUSE")
    print("="*80)

    # Collect predictions
    predictions = predictions_df.collect()
    total = len(predictions)

    print(f"Saving {total} predictions...")

    # Batch insert
    batch_size = 100
    for i in range(0, total, batch_size):
        batch = predictions[i:i+batch_size]
        values_list = []

        for row in batch:
            meets = 1 if row['predicted_et'] < ET_THRESHOLD else 0
            values = f"""(
                {row['scenario_id']},
                {row['year']},
                {row['month']},
                {row['precipitation_hours']},
                {row['sunshine_duration']},
                {row['wind_speed_10m_max']},
                {row['predicted_et']},
                {meets},
                {row['feasibility_score']},
                {row['recommendation_rank']}
            )"""
            values_list.append(values)

        insert_query = f"""
        INSERT INTO ml_predictions_may_2026 (
            scenario_id, prediction_year, prediction_month,
            precipitation_hours, sunshine_duration, wind_speed_10m_max,
            predicted_et, meets_threshold, feasibility_score, recommendation_rank
        ) VALUES {', '.join(values_list)}
        """

        execute_clickhouse_query(insert_query, verbose=False)
        print(f"✓ Saved batch {i//batch_size + 1}/{(total-1)//batch_size + 1}")

    print(f"\n✓ Successfully saved {total} predictions to ClickHouse")

# ========================================================================
# HELPER FUNCTIONS
# ========================================================================

def calculate_feasibility_score(precip, sunshine, wind):
    """
    Calculate feasibility score (0-100) for weather conditions
    Higher score = more realistic/achievable conditions

    Factors:
    - High precipitation is less common in May (penalty)
    - Very low sunshine is unusual (penalty)
    - Extreme wind speeds are rare (penalty)
    """
    score = 100.0

    # Precipitation penalty (high values are less feasible)
    if precip > 15:
        score -= (precip - 15) * 2
    elif precip > 10:
        score -= (precip - 10) * 1

    # Sunshine penalty (very low values are unusual)
    if sunshine < 3:
        score -= (3 - sunshine) * 5

    # Wind penalty (extreme values are rare)
    if wind > 20:
        score -= (wind - 20) * 3
    elif wind < 8:
        score -= (8 - wind) * 2

    return float(max(0.0, min(100.0, score)))

# ========================================================================
# MAIN PREDICTION PIPELINE
# ========================================================================

def main():
    """Main prediction pipeline"""

    print("="*80)
    print("SPARK MLLIB - ET PREDICTION FOR MAY 2026")
    print("="*80)

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
    # STEP 2: Load Trained Model
    # ────────────────────────────────────────────────────────────────────

    print("\n[STEP 2] Loading trained model from HDFS...")
    print("="*80)

    try:
        model = PipelineModel.load(MODEL_PATH)
        print(f"✓ Model loaded from: {MODEL_PATH}")

        # Extract Linear Regression model from pipeline
        lr_model = model.stages[-1]

        print("\nModel Details:")
        print(f"  Algorithm: Linear Regression")
        print(f"  Features: {FEATURE_COLS}")

        coefficients = lr_model.coefficients.toArray()
        print(f"\n  Coefficients:")
        for feature, coef in zip(FEATURE_COLS, coefficients):
            print(f"    {feature}: {coef:.6f}")
        print(f"  Intercept: {lr_model.intercept:.6f}")

    except Exception as e:
        print(f"✗ Error loading model: {e}")
        print("\nPlease run train_et_model.py first to train and save the model.")
        spark.stop()
        sys.exit(1)

    # ────────────────────────────────────────────────────────────────────
    # STEP 3: Create ClickHouse Table
    # ────────────────────────────────────────────────────────────────────

    print("\n[STEP 3] Setting up ClickHouse tables...")
    create_prediction_table()

    # ────────────────────────────────────────────────────────────────────
    # STEP 4: Generate Weather Scenarios
    # ────────────────────────────────────────────────────────────────────

    print("\n[STEP 4] Generating weather scenarios for prediction...")
    print("="*80)

    print(f"\nFeature Ranges:")
    print(f"  Precipitation hours: {min(PRECIPITATION_RANGE)}-{max(PRECIPITATION_RANGE)} hours")
    print(f"  Sunshine duration: {min(SUNSHINE_RANGE)}-{max(SUNSHINE_RANGE)} hours")
    print(f"  Wind speed: {min(WIND_SPEED_RANGE)}-{max(WIND_SPEED_RANGE)} km/h")

    # Generate all combinations (grid search)
    scenarios = []
    scenario_id = 1

    for precip, sunshine, wind in itertools.product(
        PRECIPITATION_RANGE,
        SUNSHINE_RANGE,
        WIND_SPEED_RANGE
    ):
        # Calculate feasibility score
        feasibility = calculate_feasibility_score(precip, sunshine, wind)

        scenarios.append({
            'scenario_id': scenario_id,
            'year': TARGET_YEAR,
            'month': TARGET_MONTH,
            'precipitation_hours (h)': float(precip),
            'sunshine_duration (s)': float(sunshine),
            'wind_speed_10m_max (km/h)': float(wind),
            'feasibility_score': feasibility
        })
        scenario_id += 1

    total_scenarios = len(scenarios)
    print(f"\n✓ Generated {total_scenarios:,} weather scenarios")

    # Create DataFrame
    schema = StructType([
        StructField("scenario_id", IntegerType(), False),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("precipitation_hours (h)", DoubleType(), False),
        StructField("sunshine_duration (s)", DoubleType(), False),
        StructField("wind_speed_10m_max (km/h)", DoubleType(), False),
        StructField("feasibility_score", DoubleType(), False)
    ])

    scenarios_df = spark.createDataFrame(scenarios, schema)

    print("\nSample scenarios:")
    scenarios_df.show(5, truncate=False)

    # ────────────────────────────────────────────────────────────────────
    # STEP 5: Make Predictions
    # ────────────────────────────────────────────────────────────────────

    print("\n[STEP 5] Making predictions...")
    print("="*80)

    # Apply the trained model
    predictions = model.transform(scenarios_df)

    # Round predictions for readability
    predictions = predictions.withColumn(
        "predicted_et",
        spark_round(col("prediction"), 4)
    )

    print(f"✓ Predictions completed for {total_scenarios:,} scenarios")

    print("\nSample predictions:")
    predictions.select(
        "scenario_id",
        "precipitation_hours (h)",
        "sunshine_duration (s)",
        "wind_speed_10m_max (km/h)",
        "predicted_et",
        "feasibility_score"
    ).show(10, truncate=False)

    # ────────────────────────────────────────────────────────────────────
    # STEP 6: Filter for ET < 1.5mm
    # ────────────────────────────────────────────────────────────────────

    print("\n[STEP 6] Filtering scenarios where ET < 1.5mm...")
    print("="*80)

    # Filter predictions meeting the threshold
    favorable_scenarios = predictions.filter(col("predicted_et") < ET_THRESHOLD)
    favorable_count = favorable_scenarios.count()

    print(f"\n✓ Found {favorable_count:,} scenarios with ET < {ET_THRESHOLD}mm")
    print(f"  ({favorable_count/total_scenarios*100:.2f}% of all scenarios)")

    if favorable_count == 0:
        print("\n⚠ WARNING: No scenarios found with ET < 1.5mm")
        print("   This might indicate:")
        print("   1. The threshold is too strict for May conditions")
        print("   2. The model needs retraining with more data")
        print("   3. Natural May conditions don't allow such low ET")
        spark.stop()
        return

    # Add recommendation ranking
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number

    window_spec = Window.orderBy(
        col("predicted_et").asc(),
        col("feasibility_score").desc()
    )

    favorable_ranked = favorable_scenarios.withColumn(
        "recommendation_rank",
        row_number().over(window_spec)
    )

    # ────────────────────────────────────────────────────────────────────
    # STEP 7: Display Top Recommendations
    # ────────────────────────────────────────────────────────────────────

    print("\n[STEP 7] Top Recommendations for May 2026...")
    print("="*80)

    print("\nTOP 10 RECOMMENDED WEATHER CONDITIONS")
    print("(Sorted by: Lowest ET, then Highest Feasibility)")
    print("-" * 80)

    top_10 = favorable_ranked.select(
        col("recommendation_rank").alias("Rank"),
        col("precipitation_hours (h)").alias("Precip_h"),
        col("sunshine_duration (s)").alias("Sun_h"),
        col("wind_speed_10m_max (km/h)").alias("Wind_kmh"),
        col("predicted_et").alias("ET_mm"),
        col("feasibility_score").alias("Feasibility")
    ).limit(10)

    top_10.show(10, truncate=False)

    # Get the #1 recommendation
    best = favorable_ranked.filter(col("recommendation_rank") == 1).collect()[0]

    print("\n" + "="*80)
    print("OPTIMAL RECOMMENDATION FOR MAY 2026")
    print("="*80)
    print(f"\nTo achieve evapotranspiration < {ET_THRESHOLD}mm in May 2026:")
    print(f"\n  Precipitation hours:  {best['precipitation_hours (h)']:.1f} hours")
    print(f"  Sunshine duration:    {best['sunshine_duration (s)']:.1f} hours")
    print(f"  Wind speed:           {best['wind_speed_10m_max (km/h)']:.1f} km/h")
    print(f"\n  Predicted ET:         {best['predicted_et']:.4f} mm")
    print(f"  Feasibility Score:    {best['feasibility_score']:.1f}/100")
    print("="*80)

    # ────────────────────────────────────────────────────────────────────
    # STEP 8: Statistical Analysis
    # ────────────────────────────────────────────────────────────────────

    print("\n[STEP 8] Statistical Analysis of Favorable Scenarios...")
    print("="*80)

    from pyspark.sql.functions import avg, stddev, min as spark_min, max as spark_max

    stats = favorable_ranked.agg(
        avg("precipitation_hours (h)").alias("avg_precip"),
        stddev("precipitation_hours (h)").alias("std_precip"),
        avg("sunshine_duration (s)").alias("avg_sunshine"),
        stddev("sunshine_duration (s)").alias("std_sunshine"),
        avg("wind_speed_10m_max (km/h)").alias("avg_wind"),
        stddev("wind_speed_10m_max (km/h)").alias("std_wind"),
        avg("predicted_et").alias("avg_et"),
        spark_min("predicted_et").alias("min_et"),
        spark_max("predicted_et").alias("max_et")
    ).collect()[0]

    print("\nAverage Conditions for ET < 1.5mm:")
    print(f"  Precipitation: {stats['avg_precip']:.2f} ± {stats['std_precip']:.2f} hours")
    print(f"  Sunshine: {stats['avg_sunshine']:.2f} ± {stats['std_sunshine']:.2f} hours")
    print(f"  Wind Speed: {stats['avg_wind']:.2f} ± {stats['std_wind']:.2f} km/h")
    print(f"\nPredicted ET Range:")
    print(f"  Min: {stats['min_et']:.4f} mm")
    print(f"  Avg: {stats['avg_et']:.4f} mm")
    print(f"  Max: {stats['max_et']:.4f} mm")

    # ────────────────────────────────────────────────────────────────────
    # STEP 9: Save All Predictions to ClickHouse
    # ────────────────────────────────────────────────────────────────────

    print("\n[STEP 9] Saving predictions to ClickHouse...")

    # Select and prepare data for saving
    final_predictions = predictions.select(
        "scenario_id",
        "year",
        "month",
        col("precipitation_hours (h)").alias("precipitation_hours"),
        col("sunshine_duration (s)").alias("sunshine_duration"),
        col("wind_speed_10m_max (km/h)").alias("wind_speed_10m_max"),
        "predicted_et",
        "feasibility_score"
    ).withColumn(
        "recommendation_rank",
        lit(0)  # Will be updated for favorable scenarios
    )

    # Update rank for favorable scenarios
    from pyspark.sql import Window as W
    from pyspark.sql.functions import when

    # Create ranking
    window_all = W.orderBy(
        when(col("predicted_et") < ET_THRESHOLD, 0).otherwise(1),
        col("predicted_et").asc(),
        col("feasibility_score").desc()
    )

    final_predictions = final_predictions.withColumn(
        "recommendation_rank",
        row_number().over(window_all)
    )

    # Save to ClickHouse
    save_predictions(final_predictions)

    # ────────────────────────────────────────────────────────────────────
    # STEP 10: Summary
    # ────────────────────────────────────────────────────────────────────

    print("\n" + "="*80)
    print("PREDICTION COMPLETE!")
    print("="*80)

    print("\nSummary:")
    print(f"  ✓ Model loaded from HDFS")
    print(f"  ✓ Generated {total_scenarios:,} scenarios")
    print(f"  ✓ Predictions completed")
    print(f"  ✓ Found {favorable_count:,} scenarios with ET < {ET_THRESHOLD}mm")
    print(f"  ✓ Results saved to ClickHouse")

    print("\nQuery Results in ClickHouse:")
    print("  -- Top 10 recommendations")
    print("  SELECT * FROM ml_predictions_may_2026 WHERE meets_threshold = 1 LIMIT 10;")
    print("\n  -- All favorable scenarios")
    print("  SELECT * FROM ml_predictions_may_2026 WHERE predicted_et < 1.5 ORDER BY predicted_et;")

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
        print("\n\n✗ Prediction interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
