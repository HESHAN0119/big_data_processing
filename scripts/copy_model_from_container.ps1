
# Configuration
$containerName = "project-spark-master-1"
$containerModelPath = "/tmp/et_prediction_model"
$hostModelPath = "src\spark_mllib\model"

# Check if container is running
Write-Host "[1/4] Checking Spark container status..." -ForegroundColor Yellow
$containerStatus = docker ps --filter "name=$containerName" --format "{{.Names}}"

if ($containerStatus -ne $containerName) {
    Write-Host "ERROR: Container '$containerName' is not running" -ForegroundColor Red
    exit 1
}
Write-Host "      Container is running" -ForegroundColor Green
Write-Host ""

# Check if model exists in container
Write-Host "[2/4] Checking if model exists in container..." -ForegroundColor Yellow
$modelExists = docker exec $containerName bash -c "test -d $containerModelPath && echo 'exists' || echo 'not found'"

if ($modelExists -match "not found") {
    Write-Host "ERROR: Model not found at $containerModelPath" -ForegroundColor Red
    Write-Host "       Please train the model first by running train_et_model.py" -ForegroundColor Yellow
    exit 1
}
Write-Host "      Model found: $containerModelPath" -ForegroundColor Green
Write-Host ""

# Create host directory if it doesn't exist
Write-Host "[3/4] Creating host directory..." -ForegroundColor Yellow
if (-not (Test-Path $hostModelPath)) {
    New-Item -ItemType Directory -Path $hostModelPath -Force | Out-Null
    Write-Host "      Created: $hostModelPath" -ForegroundColor Green
} else {
    Write-Host "      Directory exists: $hostModelPath" -ForegroundColor Green
    Write-Host "      Removing old model..." -ForegroundColor Yellow
    Remove-Item -Path "$hostModelPath\*" -Recurse -Force -ErrorAction SilentlyContinue
}
Write-Host ""

# Copy model from container to host
Write-Host "[4/4] Copying model from container to host..." -ForegroundColor Yellow
docker cp "${containerName}:${containerModelPath}" $hostModelPath

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Failed to copy model from container" -ForegroundColor Red
    exit 1
}

Write-Host "      Model copied successfully!" -ForegroundColor Green
Write-Host ""

# Display model contents
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "MODEL FILES COPIED" -ForegroundColor Green
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Host location: $(Resolve-Path $hostModelPath)" -ForegroundColor White
Write-Host ""
Write-Host "Model contents:" -ForegroundColor White
Get-ChildItem -Path "$hostModelPath\et_prediction_model" -Recurse | Select-Object -First 20 | ForEach-Object {
    $relativePath = $_.FullName.Replace((Resolve-Path $hostModelPath).Path + "\", "")
    if ($_.PSIsContainer) {
        Write-Host "  [DIR]  $relativePath" -ForegroundColor Cyan
    } else {
        $size = "{0:N0}" -f ($_.Length / 1KB)
        Write-Host "  [FILE] $relativePath ($size KB)" -ForegroundColor Gray
    }
}


