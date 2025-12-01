# Quick Start Guide - ML Pipeline

## 📋 **Setup (One-Time)**

### **Step 1: Copy Data to ML Training Folder**

```bash
# Create directory
docker exec namenode hdfs dfs -mkdir -p /user/data/ml_training_data/

# Copy weather data
docker exec namenode hdfs dfs -cp /user/data/kafka_ingested/weather/*.csv /user/data/ml_training_data/

# Verify
docker exec namenode hdfs dfs -ls /user/data/ml_training_data/
```

You should see CSV files listed.

---

## 🚀 **Execution**

### **Step 2: Train the Model**

```bash
# Copy training script
docker cp "f:\MSC BIG DATA\sep sem\big data processing\assignment\project\src\spark_mllib\train_et_model.py" project-spark-master-1:/tmp/

# Run training
docker exec project-spark-master-1 /spark/bin/spark-submit --master spark://spark-master:7077 /tmp/train_et_model.py
```

**Wait for:** `✓ TRAINING COMPLETE!` message

---

### **Step 3: Make Predictions**

```bash
# Copy prediction script
docker cp "f:\MSC BIG DATA\sep sem\big data processing\assignment\project\src\spark_mllib\predict_et_model.py" project-spark-master-1:/tmp/

# Run predictions
docker exec project-spark-master-1 /spark/bin/spark-submit --master spark://spark-master:7077 /tmp/predict_et_model.py
```

**Wait for:** `✓ PREDICTION COMPLETE!` message

---

## 🔍 **View Results**

```bash
# Access ClickHouse
docker exec -it clickhouse clickhouse-client

# Switch to database
USE weather_analytics;

# View model performance
SELECT * FROM ml_model_performance;

# View top 10 predictions
SELECT * FROM ml_predictions_may_2026 WHERE meets_threshold = 1 ORDER BY recommendation_rank LIMIT 10;
```

---

## 📊 **What to Expect**

### **After Training:**
- Model R² ≈ 0.80-0.85
- RMSE ≈ 0.3-0.4 mm
- Model saved to HDFS

### **After Prediction:**
- ~200-300 scenarios with ET < 1.5mm
- Top recommendation displayed
- All results in ClickHouse

---

## ⚠️ **Troubleshooting**

**"Source not found" error:**
```bash
# Check if data exists
docker exec namenode hdfs dfs -ls /user/data/ml_training_data/

# If empty, run Step 1 again
```

**"Model not found" error:**
```bash
# Run training first (Step 2)
```

**"No scenarios found" error:**
- This is normal if ET < 1.5mm is too strict
- Check prediction output for recommendations

---

## 🎯 **One-Liner (After Setup)**

```bash
# Train + Predict in sequence
docker exec project-spark-master-1 bash -c "/spark/bin/spark-submit --master spark://spark-master:7077 /tmp/train_et_model.py && /spark/bin/spark-submit --master spark://spark-master:7077 /tmp/predict_et_model.py"
```

---

**Total Time:** ~5-10 minutes for complete pipeline
