# ============================================================================
# Train ET Prediction Model and Save to Host
# ============================================================================
#
# This script:
# 1. Copies training script to Spark container
# 2. Runs model training
# 3. Copies trained model back to host (src\spark_mllib\model)
#
# Usage:
#   .\run_training.ps1
# ============================================================================

Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "ET MODEL TRAINING - SPARK MLLIB" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""

$containerName = "project-spark-master-1"
$scriptPath = "src\spark_mllib\train_et_model.py"
$containerScriptPath = "/tmp/train_et_model.py"
$containerModelPath = "/tmp/et_prediction_model"
$hostModelPath = "src\spark_mllib\model"

# Check if training script exists
if (-not (Test-Path $scriptPath)) {
    Write-Host "ERROR: Training script not found: $scriptPath" -ForegroundColor Red
    exit 1
}

Write-Host "[STEP 1/4] Copying training script to Spark container..." -ForegroundColor Yellow
docker cp $scriptPath "${containerName}:${containerScriptPath}"

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Failed to copy training script" -ForegroundColor Red
    exit 1
}
Write-Host "          Done" -ForegroundColor Green
Write-Host ""

Write-Host "[STEP 2/4] Running model training on Spark cluster..." -ForegroundColor Yellow
Write-Host "          This may take several minutes..." -ForegroundColor Gray
Write-Host "          Monitor Spark UI at: http://localhost:8080" -ForegroundColor Gray
Write-Host ""

# Run training
docker exec $containerName bash -c "/spark/bin/spark-submit --master spark://spark-master:7077 ${containerScriptPath} 2>&1"

if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "ERROR: Model training failed" -ForegroundColor Red
    Write-Host "Check the logs above for errors" -ForegroundColor Yellow
    exit 1
}

Write-Host ""
Write-Host "[STEP 3/4] Creating host directory for model..." -ForegroundColor Yellow

if (-not (Test-Path $hostModelPath)) {
    New-Item -ItemType Directory -Path $hostModelPath -Force | Out-Null
    Write-Host "          Created: $hostModelPath" -ForegroundColor Green
} else {
    Write-Host "          Directory exists, removing old model..." -ForegroundColor Yellow
    Remove-Item -Path "$hostModelPath\*" -Recurse -Force -ErrorAction SilentlyContinue
}
Write-Host ""

Write-Host "[STEP 4/4] Copying trained model from container to host..." -ForegroundColor Yellow

# Check if model exists in container
$modelExists = docker exec $containerName bash -c "test -d $containerModelPath && echo 'exists' || echo 'not found'"

if ($modelExists -match "not found") {
    Write-Host "WARNING: Model not found at $containerModelPath in container" -ForegroundColor Yellow
    Write-Host "         Model was saved to HDFS only" -ForegroundColor Yellow
} else {
    docker cp "${containerName}:${containerModelPath}" $hostModelPath

    if ($LASTEXITCODE -eq 0) {
        Write-Host "          Model copied successfully!" -ForegroundColor Green
        Write-Host ""
        Write-Host "          Model location: $(Resolve-Path $hostModelPath\et_prediction_model)" -ForegroundColor White
    } else {
        Write-Host "WARNING: Failed to copy model from container" -ForegroundColor Yellow
    }
}

Write-Host ""
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "TRAINING COMPLETE!" -ForegroundColor Green
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Model saved to:" -ForegroundColor White
Write-Host "  - HDFS:  hdfs://namenode:9000/user/models/et_prediction_model" -ForegroundColor Cyan
if (Test-Path "$hostModelPath\et_prediction_model") {
    Write-Host "  - Local: $(Resolve-Path $hostModelPath\et_prediction_model)" -ForegroundColor Cyan
}
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  1. Run predictions: .\run_manual_prediction.ps1" -ForegroundColor White
Write-Host "  2. View Spark UI: http://localhost:8080" -ForegroundColor White
Write-Host "================================================================================" -ForegroundColor Cyan
