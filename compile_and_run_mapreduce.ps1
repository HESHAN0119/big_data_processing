# PowerShell script to compile and run MapReduce jobs

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "Compiling MapReduce Jobs" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

# Copy Java source files to namenode
Write-Host "`nCopying Java files to namenode..." -ForegroundColor Yellow
docker cp src/mapreduce/DistrictMonthlyWeather.java namenode:/tmp/
docker cp src/mapreduce/HighestPrecipitationMonth.java namenode:/tmp/

# Compile Job 1: DistrictMonthlyWeather
Write-Host "`nCompiling DistrictMonthlyWeather.java..." -ForegroundColor Yellow
docker exec namenode bash -c "mkdir -p /tmp/district_monthly_classes && cd /tmp && javac -classpath `$(hadoop classpath) -d /tmp/district_monthly_classes DistrictMonthlyWeather.java"

# Create JAR for Job 1
Write-Host "Creating JAR for DistrictMonthlyWeather..." -ForegroundColor Yellow
docker exec namenode bash -c "cd /tmp/district_monthly_classes && jar -cvf /tmp/district-monthly-weather.jar *.class" | Out-Null

# Compile Job 2: HighestPrecipitationMonth
Write-Host "`nCompiling HighestPrecipitationMonth.java..." -ForegroundColor Yellow
docker exec namenode bash -c "mkdir -p /tmp/highest_precip_classes && cd /tmp && javac -classpath `$(hadoop classpath) -d /tmp/highest_precip_classes HighestPrecipitationMonth.java"

# Create JAR for Job 2
Write-Host "Creating JAR for HighestPrecipitationMonth..." -ForegroundColor Yellow
docker exec namenode bash -c "cd /tmp/highest_precip_classes && jar -cvf /tmp/highest-precipitation-month.jar *.class" | Out-Null

Write-Host "`n============================================" -ForegroundColor Cyan
Write-Host "Running MapReduce Job 1: District Monthly Weather" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

# Remove output directory if exists
# docker exec namenode bash -c 'hdfs dfs -rm -r /user/data/mapreduce_output/district_monthly 2>/dev/null || true' | Out-Null

# Run Job 1
Write-Host "`nRunning MapReduce job (this may take a few minutes)..." -ForegroundColor Yellow
$job1Result = docker exec namenode bash -c "hadoop jar /tmp/district-monthly-weather.jar DistrictMonthlyWeather /user/data/kafka_ingested/location /user/data/kafka_ingested/weather /user/data/mapreduce_output/district_monthly"

if ($LASTEXITCODE -eq 0) {
    Write-Host "`nResults (first 20 lines):" -ForegroundColor Green
    Write-Host "--------------------------------" -ForegroundColor Green
    docker exec namenode bash -c 'hdfs dfs -cat /user/data/mapreduce_output/district_monthly/mean_temp_*/part-* 2>/dev/null | head -20 || hdfs dfs -cat /user/data/mapreduce_output/district_monthly/part-* 2>/dev/null | head -20'
    $job1Success = $true
} else {
    Write-Host "`n✗ Job 1 failed or was skipped due to empty input directories" -ForegroundColor Red
    $job1Success = $false
}

Write-Host "`n============================================" -ForegroundColor Cyan
Write-Host "Running MapReduce Job 2: Highest Precipitation Month" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

# Run Job 2
Write-Host "`nRunning MapReduce job..." -ForegroundColor Yellow
$job2Result = docker exec namenode bash -c "hadoop jar /tmp/highest-precipitation-month.jar HighestPrecipitationMonth /user/data/kafka_ingested/weather /user/data/mapreduce_output/highest_precipitation"

if ($LASTEXITCODE -eq 0) {
    Write-Host "`nResult:" -ForegroundColor Green
    Write-Host "--------------------------------" -ForegroundColor Green
    docker exec namenode bash -c 'hdfs dfs -cat /user/data/mapreduce_output/highest_precipitation/hpm_*/part-* 2>/dev/null | head -20 || hdfs dfs -cat /user/data/mapreduce_output/highest_precipitation/part-* 2>/dev/null | head -20'
    $job2Success = $true
} else {
    Write-Host "`n✗ Job 2 failed or was skipped due to empty input directories" -ForegroundColor Red
    $job2Success = $false
}

Write-Host "`n============================================" -ForegroundColor Cyan
Write-Host "All MapReduce jobs completed!" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Cyan

# Clean up weather data files only if BOTH jobs succeeded
if ($job1Success -and $job2Success) {
    Write-Host "`n[*] Cleaning up processed weather data files..." -ForegroundColor Yellow
    docker exec namenode bash -c 'hdfs dfs -rm /user/data/kafka_ingested/weather/* 2>/dev/null'
    if ($LASTEXITCODE -eq 0) {
        Write-Host "    [OK] Weather data files deleted successfully" -ForegroundColor Green
    } else {
        Write-Host "    [WARNING] No weather files to delete or deletion failed" -ForegroundColor Yellow
    }
} else {
    Write-Host "`n[*] Skipping cleanup - Not all jobs completed successfully" -ForegroundColor Yellow
}

Write-Host "`nOutput locations in HDFS:" -ForegroundColor Yellow
Write-Host "  - District Monthly Weather: /user/data/mapreduce_output/district_monthly/" -ForegroundColor White
Write-Host "  - Highest Precipitation Month: /user/data/mapreduce_output/highest_precipitation/" -ForegroundColor White
