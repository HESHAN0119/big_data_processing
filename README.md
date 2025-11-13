# Weather Data Pipeline - Lambda Architecture

Complete Big Data pipeline processing weather data through Kafka, HDFS, MapReduce, and ClickHouse.

## Architecture

```
CSV Files → Kafka → HDFS → MapReduce → ClickHouse → Web UI
```

## Quick Start

### 1. Start All Services
```powershell
docker-compose up -d
```

### 2. Upload Data
Start Kafka services in separate terminals:
```powershell
# Terminal 1
python src/kafka/file_watcher_producer.py

# Terminal 2
python src/kafka/kafka_hdfs_consumer.py
```

Copy your CSV files to the `data/` folder. Kafka will automatically process them to HDFS.

### 3. Run Pipeline
```powershell
.\automate_pipeline.ps1
```

This will:
1. Check HDFS for data
2. Run MapReduce jobs
3. Load results into ClickHouse

### 4. View Results
Open http://localhost:8123/play

**Login:**
- Username: `default`
- Password: `clickhouse123`
- Database: `weather_analytics`

**Sample Query:**
```sql
SELECT district, year_month, total_precipitation_hours, mean_temperature
FROM weather_analytics.district_monthly_weather
ORDER BY district, year, month
LIMIT 20;
```

## Project Structure

```
project/
├── data/                           # CSV files (watched by Kafka producer)
├── src/
│   ├── kafka/                      # Kafka producer/consumer scripts
│   ├── mapreduce/                  # MapReduce Java jobs
│   ├── clickhouse/                 # ClickHouse schema
│   └── spark/                      # Spark analysis (if implemented)
├── docker-compose.yml              # All services configuration
├── compile_and_run_mapreduce.ps1   # Runs MapReduce jobs
├── automate_pipeline.ps1           # Complete automation script
└── watch_and_process_simple.ps1    # HDFS monitor (optional)
```

## Components

### 1. Kafka Ingestion Layer
- **Producer**: Monitors `data/` folder, streams CSV to Kafka
- **Consumer**: Reads from Kafka, writes to HDFS

### 2. Batch Processing (MapReduce)
- **Job 1**: District Monthly Weather - precipitation & temperature by district/month
- **Job 2**: Highest Precipitation Month - finds peak precipitation month

### 3. Data Warehouse (ClickHouse)
- Stores processed results
- Provides SQL interface
- Web UI for analytics

## Troubleshooting

**No data in HDFS:**
```powershell
# Check if files exist
docker exec namenode bash -c "hdfs dfs -ls /user/data/kafka_ingested/"
```

**MapReduce fails:**
```powershell
# Check HDFS data first
docker exec namenode bash -c "hdfs dfs -cat /user/data/kafka_ingested/weatherData.csv | head"

# Check logs
docker logs namenode
```

**ClickHouse empty:**
```powershell
# Verify count
docker exec clickhouse clickhouse-client --user=default --password=clickhouse123 --query='SELECT count() FROM weather_analytics.district_monthly_weather'
```

## Services

- **Kafka**: localhost:9092
- **Zookeeper**: localhost:2181
- **HDFS NameNode**: localhost:9870
- **YARN ResourceManager**: localhost:8088
- **ClickHouse HTTP**: localhost:8123
- **ClickHouse Native**: localhost:9001

## Documentation

- `QUICKSTART.md` - Step-by-step setup guide
- `AUTOMATION_SOLUTION.md` - Detailed pipeline explanation

## Tech Stack

- **Apache Kafka** - Stream processing
- **Apache Hadoop** - Distributed storage (HDFS) & processing (MapReduce)
- **ClickHouse** - Columnar data warehouse
- **Docker** - Container orchestration
- **Python** - Kafka scripts
- **Java** - MapReduce jobs
- **PowerShell** - Automation scripts
