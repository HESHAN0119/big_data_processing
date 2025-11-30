import sys
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

# Input your weather conditions here:
PRECIPITATION_HOURS = 15.0           # Hours of precipitation
SUNSHINE_DURATION = 6.0             # Hours of sunshine (already converted from seconds)
WIND_SPEED_10M_MAX = 12.0          # Wind speed in km/h


# Model Paths
# Option 1: Load from local filesystem (mounted from host)
LOCAL_MODEL_PATH = "/tmp/model/et_prediction_model"  # Will be mounted from host

# Option 2: Load from HDFS (if needed)
HDFS_BASE = "hdfs://namenode:9000"
HDFS_MODEL_PATH = f"{HDFS_BASE}/user/models/et_prediction_model"

# Use local model by default
MODEL_PATH = f"file://{LOCAL_MODEL_PATH}"

# Spark Configuration
SPARK_APP_NAME = "ET Manual Prediction"
SPARK_MASTER = "spark://spark-master:7077"

# Feature column names (must match training data)
FEATURE_COLS = [
    'precipitation_hours (h)',
    'sunshine_duration (s)',
    'wind_speed_10m_max (km/h)'
]

def main():
    """Main prediction function"""

    print("="*80)
    print("SPARK MLLIB - MANUAL ET PREDICTION")
    print("="*80)


    print("\n[STEP 1] Initializing Spark Session...")

    spark = SparkSession.builder \
        .appName(SPARK_APP_NAME) \
        .master(SPARK_MASTER) \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.cores.max", "2") \
        .getOrCreate()

    print(f"✓ Spark Session initialized")
    print(f"  App ID: {spark.sparkContext.applicationId}")

    print("\n[STEP 2] Loading trained model from HDFS...")
    print(f"  Model path: {MODEL_PATH}")

    try:
        model = PipelineModel.load(MODEL_PATH)
        print("✓ Model loaded successfully")

        # Extract Linear Regression model (last stage in pipeline)
        lr_model = model.stages[-1]

        # Display model coefficients
        print("\n📐 Model Coefficients:")
        coefficients = lr_model.coefficients.toArray()
        for feature, coef in zip(FEATURE_COLS, coefficients):
            print(f"  {feature:45s}: {coef:+.6f}")
        print(f"  {'Intercept':45s}: {lr_model.intercept:+.6f}")

    except Exception as e:
        print(f"✗ Error loading model: {e}")
        spark.stop()
        sys.exit(1)

    print("\n" + "="*80)
    print("INPUT WEATHER CONDITIONS")
    print("="*80)
    print(f"Precipitation Hours:       {PRECIPITATION_HOURS} hours")
    print(f"Sunshine Duration:         {SUNSHINE_DURATION} hours")
    print(f"Wind Speed (10m max):      {WIND_SPEED_10M_MAX} km/h")
    print("="*80)


    # STEP 4: Create DataFrame with Input Data


    print("\n[STEP 3] Creating input DataFrame...")

    # Create a single-row DataFrame with the input values
    input_data = [(PRECIPITATION_HOURS, SUNSHINE_DURATION, WIND_SPEED_10M_MAX)]

    input_df = spark.createDataFrame(
        input_data,
        schema=FEATURE_COLS
    )

    print("✓ Input DataFrame created")
    print("\n📊 Input data:")
    input_df.show(truncate=False)


    # STEP 5: Make Prediction
   

    print("\n[STEP 4] Making ET prediction...")

    # Apply the model to make prediction
    prediction = model.transform(input_df)

    # Extract the predicted value
    predicted_et = prediction.select("prediction").collect()[0][0]

    print("✓ Prediction complete")

   
    # STEP 6: Display Results


    print("\n" + "="*80)
    print("PREDICTION RESULTS")
    print("="*80)
    print(f"Predicted Evapotranspiration (ET):  {predicted_et:.4f} mm")
    print("="*80)

    # Check if ET is below target threshold
    target_threshold = 1.5  # mm

    print("\n🎯 EVALUATION")
    print("="*80)
    print(f"Target Threshold:  {target_threshold} mm")
    print(f"Predicted ET:      {predicted_et:.4f} mm")

    if predicted_et < target_threshold:
        print(f"\n✅ SUCCESS! ET is below {target_threshold} mm")
        print(f"   Difference: {target_threshold - predicted_et:.4f} mm below threshold")
    else:
        print(f"\n❌ ET exceeds {target_threshold} mm threshold")
        print(f"   Difference: {predicted_et - target_threshold:.4f} mm above threshold")

    print("="*80)


    # STEP 7: Show Complete Prediction Details

    print("\n[STEP 5] Complete prediction details:")
    prediction.select(
        *FEATURE_COLS,
        "prediction"
    ).show(truncate=False)


    # Summary

    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"✓ Model loaded from:        {MODEL_PATH}")
    print(f"✓ Input conditions:")
    print(f"    • Precipitation:        {PRECIPITATION_HOURS} hours")
    print(f"    • Sunshine:             {SUNSHINE_DURATION} hours")
    print(f"    • Wind Speed:           {WIND_SPEED_10M_MAX} km/h")
    print(f"✓ Predicted ET:             {predicted_et:.4f} mm")
    print(f"✓ Target threshold:         {target_threshold} mm")
    if predicted_et < target_threshold:
        print(f"✓ Status:                   ✅ BELOW THRESHOLD")
    else:
        print(f"✓ Status:                   ❌ ABOVE THRESHOLD")
    print("="*80)

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
