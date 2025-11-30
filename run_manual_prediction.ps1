# ============================================================================
# Manual ET Prediction Runner
# ============================================================================
#
# This script copies and runs the manual ET prediction on Spark
#
# Usage:
#   1. Edit values in src/spark_mllib/predict_et_manual.py (lines 17-19)
#   2. Run this script: .\run_manual_prediction.ps1
# ============================================================================

Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "MANUAL ET PREDICTION - SPARK RUNNER" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""

# Check if file exists
$scriptPath = "src\spark_mllib\predict_et_manual.py"
if (-not (Test-Path $scriptPath)) {
    Write-Host "ERROR: File not found: $scriptPath" -ForegroundColor Red
    exit 1
}

Write-Host "[1/4] Checking if model exists on host..." -ForegroundColor Yellow
$modelPath = "src\spark_mllib\model\et_prediction_model"
if (-not (Test-Path $modelPath)) {
    Write-Host "ERROR: Model not found at $modelPath" -ForegroundColor Red
    Write-Host "       Please train the model first: .\run_training.ps1" -ForegroundColor Yellow
    exit 1
}
Write-Host "      Model found: $modelPath" -ForegroundColor Green
Write-Host ""

Write-Host "[2/4] Copying model to Spark container..." -ForegroundColor Yellow
docker exec project-spark-master-1 mkdir -p /tmp/model
docker cp $modelPath project-spark-master-1:/tmp/model/

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Failed to copy model to container" -ForegroundColor Red
    exit 1
}
Write-Host "      Model copied to container" -ForegroundColor Green
Write-Host ""

Write-Host "[3/4] Copying prediction script to Spark container..." -ForegroundColor Yellow
docker cp $scriptPath project-spark-master-1:/tmp/predict_et_manual.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Failed to copy file to container" -ForegroundColor Red
    exit 1
}
Write-Host "      Done" -ForegroundColor Green
Write-Host ""

Write-Host "[4/4] Running prediction on Spark cluster..." -ForegroundColor Yellow
Write-Host "      This may take a few moments..." -ForegroundColor Gray
Write-Host ""

# Run the prediction
docker exec project-spark-master-1 bash -c "/spark/bin/spark-submit --master spark://spark-master:7077 /tmp/predict_et_manual.py 2>&1"

if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "ERROR: Prediction failed" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "✅ Prediction complete!" -ForegroundColor Green
Write-Host ""
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "To run with different values:" -ForegroundColor Cyan
Write-Host "  1. Edit src\spark_mllib\predict_et_manual.py (lines 17-19)" -ForegroundColor White
Write-Host "  2. Run: .\run_manual_prediction.ps1" -ForegroundColor White
Write-Host "================================================================================" -ForegroundColor Cyan
