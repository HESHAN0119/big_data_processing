# Data Analysis Tables Mapping Documentation

**Project:** Sri Lankan Weather Data Analytics
**Database:** ClickHouse - `weather_analytics`
**Total Tables:** 10
**Total Records:** 147,070+ records

---

## Table of Contents
1. [MapReduce Tables (Task 2.1)](#task-21---hadoop-mapreduce-analysis)
2. [Hive Tables (Task 2.2)](#task-22---hive-analysis)
3. [Spark Tables (Task 2.3)](#task-23---spark-analysis)
4. [Spark MLlib Tables (Task 3)](#task-3---spark-mllib-machine-learning)
5. [Supporting Tables](#supporting-tables)
6. [Data Flow Diagram](#data-flow-diagram)

---

## Task 2.1 - Hadoop MapReduce Analysis

### **Analysis Requirements:**
1. Calculate the total precipitation and mean temperature for each district per month
2. Find the month and year with the highest total precipitation in the full dataset

### **Related Tables:**

#### 1️⃣ **`district_monthly_weather`**
**Purpose:** Stores monthly aggregated weather data per district
**Source:** MapReduce jobs → HDFS → Python loader → ClickHouse
**Records:** 4,698

**Schema:**
```sql
CREATE TABLE district_monthly_weather (
    district                  String,   -- District name (e.g., Colombo, Gampaha)
    year                      UInt16,   -- Year (2010-2024)
    month                     UInt8,    -- Month (1-12)
    year_month                String,   -- Combined format (YYYY-MM)
    total_precipitation_hours Float64,  -- Total precipitation hours in the month
    mean_temperature          Float64,  -- Mean temperature (°C) for the month
    created_at                DateTime  -- Insertion timestamp
) ENGINE = MergeTree()
ORDER BY (district, year, month);
```

**Sample Data:**
| district | year | month | year_month | total_precipitation_hours | mean_temperature |
|----------|------|-------|------------|---------------------------|------------------|
| Colombo  | 2019 | 2     | 2019-02    | 30.5                      | 25.3             |
| Gampaha  | 2020 | 5     | 2020-05    | 45.2                      | 28.1             |

**MapReduce Job:**
- **Java Class:** `DistrictMonthlyWeather.java`
- **Input:** HDFS `/user/data/kafka_ingested/weather/` + `/user/data/kafka_ingested/location/`
- **Output:** HDFS `/user/data/mapreduce_output/district_monthly/mean_temp_<timestamp>/`
- **Loader Script:** `load_mapreduce_output.py` → `load_district_monthly_weather()`

**Used By:**
- Dashboard Page 1: Precipitation Analysis
- Dashboard Page 2: Top 5 Districts

---

#### 2️⃣ **`highest_precipitation`**
**Purpose:** Stores the month/year with highest total precipitation
**Source:** MapReduce job → HDFS → Python loader → ClickHouse
**Records:** 1

**Schema:**
```sql
CREATE TABLE highest_precipitation (
    year                      UInt16,   -- Year with highest precipitation
    month                     UInt8,    -- Month with highest precipitation
    year_month                String,   -- Combined format (YYYY-MM)
    total_precipitation_hours Float64,  -- Total precipitation hours
    created_at                DateTime  -- Insertion timestamp
) ENGINE = MergeTree()
ORDER BY (year, month);
```

**Sample Data:**
| year | month | year_month | total_precipitation_hours |
|------|-------|------------|---------------------------|
| 2019 | 2     | 2019-02    | 300.0                     |

**MapReduce Job:**
- **Java Class:** `HighestPrecipitationMonth.java`
- **Input:** HDFS `/user/data/kafka_ingested/weather/`
- **Output:** HDFS `/user/data/mapreduce_output/highest_precipitation/hpm_<timestamp>/`
- **Loader Script:** `load_mapreduce_output.py` → `load_highest_precipitation()`

---

## Task 2.2 - Hive Analysis

### **Analysis Requirements:**
1. Rank the top 10 most temperate cities across the dataset (use temperature_2m_max)
2. Calculate the average evapotranspiration for each major agricultural season (Maha & Yala)

### **Related Tables:**

#### 3️⃣ **`top_temperate_cities`**
**Purpose:** Top 10 cities with highest average maximum temperatures
**Source:** Hive query → Python script → ClickHouse
**Records:** 10

**Schema:**
```sql
CREATE TABLE top_temperate_cities (
    city_name          String,   -- City/district name
    avg_max_temp       Float64,  -- Average maximum temperature (°C)
    analysis_timestamp DateTime, -- When analysis was run
    created_at         DateTime  -- Insertion timestamp
) ENGINE = MergeTree()
ORDER BY (avg_max_temp);
```

**Sample Data:**
| city_name | avg_max_temp | analysis_timestamp      |
|-----------|--------------|-------------------------|
| Jaffna    | 32.5         | 2024-11-13 10:30:00     |
| Colombo   | 31.8         | 2024-11-13 10:30:00     |
| Gampaha   | 31.2         | 2024-11-13 10:30:00     |

**Hive Query:**
- **File:** `src/hive/02_query1_top_cities.hql`
- **Input Tables:** `weather_data`, `location_data` (Hive external tables pointing to HDFS)
- **Output:** HDFS `/user/data/hive_output/top_10_temperate_cities_<timestamp>/`
- **Loader Script:** `run_hive_analysis_simple.py` → Loads result to ClickHouse

**Used By:**
- Dashboard Page 3: Temperature Analysis

---

#### 4️⃣ **`evapotranspiration_by_season`**
**Purpose:** Average evapotranspiration by agricultural season (Maha/Yala) per district
**Source:** Hive query → Python script → ClickHouse
**Records:** 783

**Schema:**
```sql
CREATE TABLE evapotranspiration_by_season (
    city_name              String,   -- City/district name
    season                 String,   -- 'Maha (Sep-Mar)' or 'Yala (Apr-Aug)'
    year                   UInt16,   -- Year
    avg_evapotranspiration Float64,  -- Average ET0 (mm/day)
    analysis_timestamp     DateTime, -- When analysis was run
    created_at             DateTime  -- Insertion timestamp
) ENGINE = MergeTree()
ORDER BY (city_name, year, season);
```

**Sample Data:**
| city_name | season           | year | avg_evapotranspiration | analysis_timestamp  |
|-----------|------------------|------|------------------------|---------------------|
| Colombo   | Maha (Sep-Mar)   | 2019 | 4.5                    | 2024-11-13 10:30:00 |
| Colombo   | Yala (Apr-Aug)   | 2019 | 5.2                    | 2024-11-13 10:30:00 |

**Agricultural Seasons:**
- **Maha (Main Season):** September to March (months: 9, 10, 11, 12, 1, 2, 3)
- **Yala (Inter-Season):** April to August (months: 4, 5, 6, 7, 8)

**Hive Query:**
- **File:** `src/hive/03_query2_evapotranspiration.hql`
- **Input Tables:** `weather_data`, `location_data`
- **Output:** HDFS `/user/data/hive_output/avg_evapotranspiration_by_season_<timestamp>/`
- **Loader Script:** `run_hive_analysis_simple.py`

**Used By:**
- Dashboard analytics (Page 1 seasonal comparison)

---

## Task 2.3 - Spark Analysis

### **Analysis Requirements:**
1. Calculate the percentage of total shortwave radiation which is more than 15MJ/m² in a month
2. The weekly maximum temperatures for the hottest months of an year

### **Related Tables:**

#### 5️⃣ **`radiation_analysis`**
**Purpose:** Monthly shortwave radiation analysis (percentage > 15 MJ/m²)
**Source:** PySpark → ClickHouse HTTP API
**Records:** 180 (15 years × 12 months)

**Schema:**
```sql
CREATE TABLE radiation_analysis (
    year                UInt16,   -- Year
    month               UInt8,    -- Month (1-12)
    total_days          UInt32,   -- Total days in dataset for that month
    days_above_15       UInt32,   -- Days with radiation > 15 MJ/m²
    percentage_above_15 Float64,  -- Percentage (days_above_15 / total_days * 100)
    avg_radiation       Float64,  -- Average radiation for the month (MJ/m²)
    created_at          DateTime  -- Insertion timestamp
) ENGINE = MergeTree()
ORDER BY (year, month);
```

**Sample Data:**
| year | month | total_days | days_above_15 | percentage_above_15 | avg_radiation |
|------|-------|------------|---------------|---------------------|---------------|
| 2019 | 5     | 837        | 625           | 74.67               | 18.5          |
| 2020 | 8     | 837        | 710           | 84.83               | 19.2          |

**Spark Job:**
- **Script:** `src/spark/weather_spark_analysis_new.py`
- **Function:** `analyze_radiation()`
- **Input:** HDFS `/user/data/kafka_ingested/weather/`
- **Output:** Direct HTTP POST to ClickHouse

**Used By:**
- Dashboard Page 3: Radiation analysis charts

---

#### 6️⃣ **`weekly_max_temp_hottest_months`**
**Purpose:** Weekly maximum temperatures for the hottest months of each year
**Source:** PySpark → ClickHouse HTTP API
**Records:** 2,700+

**Schema:**
```sql
CREATE TABLE weekly_max_temp_hottest_months (
    year            UInt16,   -- Year
    month           UInt8,    -- Hottest month of that year
    week            UInt8,    -- Week number in the month
    city_name       String,   -- City/district name
    weekly_max_temp Float64,  -- Maximum temperature for that week (°C)
    weekly_avg_temp Float64,  -- Average temperature for that week (°C)
    days_in_week    UInt8,    -- Number of days in that week
    created_at      DateTime  -- Insertion timestamp
) ENGINE = MergeTree()
ORDER BY (year, month, week, city_name);
```

**Sample Data:**
| year | month | week | city_name | weekly_max_temp | weekly_avg_temp | days_in_week |
|------|-------|------|-----------|-----------------|-----------------|--------------|
| 2019 | 5     | 1    | Colombo   | 35.2            | 32.1            | 7            |
| 2019 | 5     | 2    | Gampaha   | 34.8            | 31.5            | 7            |

**Spark Job:**
- **Script:** `src/spark/weather_spark_analysis_new.py`
- **Function:** `analyze_weekly_max_temp_hottest_months()`
- **Input:** HDFS `/user/data/kafka_ingested/weather/` + `/user/data/kafka_ingested/location/`
- **Output:** Direct HTTP POST to ClickHouse

**Used By:**
- Dashboard Page 3: Weekly temperature trends

---

## Task 3 - Spark MLlib Machine Learning

### **Goal:**
Predict precipitation_hours, sunshine_duration, and wind_speed that lead to lower evapotranspiration for May.

### **Model Details:**
- **Algorithm:** Linear Regression
- **Features:** precipitation_hours, sunshine_duration, wind_speed_10m_max
- **Target:** et0_fao_evapotranspiration
- **Train/Test Split:** 80% / 20%

### **Related Tables:**

#### 7️⃣ **`ml_feature_statistics`**
**Purpose:** Statistical summary of ML features by month
**Source:** PySpark MLlib training script
**Records:** 48 (12 months × 4 features)

**Schema:**
```sql
CREATE TABLE ml_feature_statistics (
    month        UInt8,    -- Month (1-12)
    feature_name String,   -- Feature name
    mean_value   Float64,  -- Mean value
    std_dev      Float64,  -- Standard deviation
    min_value    Float64,  -- Minimum value
    max_value    Float64,  -- Maximum value
    sample_count UInt32,   -- Number of samples
    created_at   DateTime  -- Insertion timestamp
) ENGINE = MergeTree()
ORDER BY (month, feature_name);
```

**Sample Data:**
| month | feature_name        | mean_value | std_dev | min_value | max_value | sample_count |
|-------|---------------------|------------|---------|-----------|-----------|--------------|
| 5     | precipitation_hours | 3.5        | 2.1     | 0.0       | 12.0      | 4185         |
| 5     | sunshine_duration   | 25000.0    | 8000.0  | 5000.0    | 45000.0   | 4185         |
| 5     | wind_speed_10m_max  | 18.5       | 4.2     | 8.0       | 32.0      | 4185         |
| 5     | et0_fao_evapotrans  | 4.8        | 1.2     | 1.5       | 8.5       | 4185         |

**ML Script:**
- **Training:** `src/spark_mllib/train_et_model.py`
- **Output:** Metadata saved during training

**Used By:**
- ML model evaluation and feature analysis

---

#### 8️⃣ **`ml_model_performance`**
**Purpose:** Stores ML model training performance metrics
**Source:** PySpark MLlib training script
**Records:** 1 per training run

**Schema:**
```sql
CREATE TABLE ml_model_performance (
    model_name    String,   -- Model identifier
    training_date DateTime, -- When model was trained
    train_size    UInt32,   -- Number of training samples
    test_size     UInt32,   -- Number of test samples
    rmse          Float64,  -- Root Mean Squared Error
    r2            Float64,  -- R² score
    mae           Float64,  -- Mean Absolute Error
    feature_1     String,   -- Feature 1 name
    feature_2     String,   -- Feature 2 name
    feature_3     String,   -- Feature 3 name
    coefficient_1 Float64,  -- Coefficient for feature 1
    coefficient_2 Float64,  -- Coefficient for feature 2
    coefficient_3 Float64,  -- Coefficient for feature 3
    intercept     Float64   -- Model intercept
) ENGINE = MergeTree()
ORDER BY (training_date);
```

**Sample Data:**
| model_name           | training_date       | train_size | test_size | rmse | r2   | mae  |
|---------------------|---------------------|------------|-----------|------|------|------|
| ET_Prediction_Model | 2024-11-13 15:30:00 | 113,897    | 28,475    | 0.82 | 0.73 | 0.61 |

**Model Coefficients:**
| feature_1           | feature_2         | feature_3          | coefficient_1 | coefficient_2 | coefficient_3 | intercept |
|---------------------|-------------------|--------------------|---------------|---------------|---------------|-----------|
| precipitation_hours | sunshine_duration | wind_speed_10m_max | -0.0234       | 0.000045      | 0.0421        | 2.15      |

**ML Script:**
- **Training:** `src/spark_mllib/train_et_model.py`
- **Model Save Location:** `src/spark_mllib/model/et_prediction_model/`

**Interpretation:**
- Negative coefficient for precipitation_hours: More rain → Less evapotranspiration
- Positive coefficient for sunshine: More sun → More evapotranspiration
- Positive coefficient for wind: More wind → More evapotranspiration

**Used By:**
- Model evaluation
- Prediction scripts

---

## Supporting Tables

### 9️⃣ **`raw_weather_data`**
**Purpose:** Raw daily weather observations for all locations
**Source:** CSV files → Dashboard data loader
**Records:** 142,372

**Schema:**
```sql
CREATE TABLE raw_weather_data (
    location_id                 UInt32,  -- Location ID (1-27)
    date                        Date,    -- Observation date
    weather_code                UInt16,  -- WMO weather code
    temperature_2m_max          Float64, -- Max temperature (°C)
    temperature_2m_min          Float64, -- Min temperature (°C)
    temperature_2m_mean         Float64, -- Mean temperature (°C)
    apparent_temperature_max    Float64, -- Feels-like max temp (°C)
    apparent_temperature_min    Float64, -- Feels-like min temp (°C)
    apparent_temperature_mean   Float64, -- Feels-like mean temp (°C)
    daylight_duration           Float64, -- Daylight hours (seconds)
    sunshine_duration           Float64, -- Sunshine hours (seconds)
    precipitation_sum           Float64, -- Total precipitation (mm)
    rain_sum                    Float64, -- Total rain (mm)
    precipitation_hours         Float64, -- Hours of precipitation
    wind_speed_10m_max          Float64, -- Max wind speed (km/h)
    wind_gusts_10m_max          Float64, -- Max wind gusts (km/h)
    wind_direction_10m_dominant Float64, -- Dominant wind direction (degrees)
    shortwave_radiation_sum     Float64, -- Total solar radiation (MJ/m²)
    et0_fao_evapotranspiration  Float64, -- Reference evapotranspiration (mm)
    created_at                  DateTime -- Insertion timestamp
) ENGINE = MergeTree()
ORDER BY (location_id, date);
```

**Date Range:** 2010-01-01 to 2024-12-31
**Locations:** 27 districts in Sri Lanka

**Used By:**
- Dashboard Page 4: Extreme Weather Events
- Source data for Kafka streaming
- ML model training data

---

### 🔟 **`locations`**
**Purpose:** Geographic information for all weather stations
**Source:** CSV file → Dashboard data loader
**Records:** 27

**Schema:**
```sql
CREATE TABLE locations (
    location_id           UInt32,  -- Unique location ID
    latitude              Float64, -- Latitude (degrees N)
    longitude             Float64, -- Longitude (degrees E)
    elevation             Float64, -- Elevation (meters)
    utc_offset_seconds    Int32,   -- UTC offset (seconds)
    timezone              String,  -- Timezone (e.g., Asia/Colombo)
    timezone_abbreviation String,  -- Timezone abbreviation (IST)
    city_name             String,  -- City/district name
    created_at            DateTime -- Insertion timestamp
) ENGINE = MergeTree()
ORDER BY (location_id);
```

**Sample Data:**
| location_id | latitude | longitude | elevation | city_name |
|-------------|----------|-----------|-----------|-----------|
| 1           | 6.93     | 79.85     | 8.0       | Colombo   |
| 2           | 7.00     | 79.90     | 15.0      | Gampaha   |
| 3           | 9.66     | 80.02     | 5.0       | Jaffna    |

**Used By:**
- All analysis tasks (joins with weather data)
- Dashboard: District mapping
- Geographic visualizations

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                  │
├─────────────────────────────────────────────────────────────────────┤
│  CSV Files: weatherData.csv (142K records)                           │
│             locationData.csv (27 records)                            │
└────────────────────────┬────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   KAFKA STREAMING PIPELINE                           │
├─────────────────────────────────────────────────────────────────────┤
│  Producer: file_watcher_producer.py                                 │
│  Consumer: kafka_hdfs_consumer.py                                   │
│  Output: HDFS /user/data/kafka_ingested/                            │
└─────┬───────────────────────┬───────────────────────┬───────────────┘
      │                       │                       │
      ▼                       ▼                       ▼
┌─────────────┐    ┌──────────────────┐    ┌────────────────────┐
│ MAPREDUCE   │    │      HIVE        │    │      SPARK         │
│  (Task 2.1) │    │   (Task 2.2)     │    │   (Task 2.3)       │
├─────────────┤    ├──────────────────┤    ├────────────────────┤
│ Java Jobs:  │    │ HiveQL Queries:  │    │ PySpark Scripts:   │
│             │    │                  │    │                    │
│ • District  │    │ • Top 10 Temp.   │    │ • Radiation %      │
│   Monthly   │    │   Cities         │    │ • Weekly Max Temp  │
│   Weather   │    │                  │    │   (Hottest Months) │
│             │    │ • Seasonal ET    │    │                    │
│ • Highest   │    │   (Maha/Yala)    │    │                    │
│   Precip.   │    │                  │    │                    │
│   Month     │    │                  │    │                    │
└──────┬──────┘    └────────┬─────────┘    └─────────┬──────────┘
       │                    │                        │
       │ HDFS Output        │ HDFS Output            │ HTTP POST
       │                    │                        │
       ▼                    ▼                        ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      CLICKHOUSE DATABASE                             │
│                   weather_analytics Database                         │
├─────────────────────────────────────────────────────────────────────┤
│  MapReduce Tables:                                                   │
│    ✓ district_monthly_weather (4,698 records)                       │
│    ✓ highest_precipitation (1 record)                               │
│                                                                      │
│  Hive Tables:                                                        │
│    ✓ top_temperate_cities (10 records)                              │
│    ✓ evapotranspiration_by_season (783 records)                     │
│                                                                      │
│  Spark Tables:                                                       │
│    ✓ radiation_analysis (180 records)                               │
│    ✓ weekly_max_temp_hottest_months (2,700+ records)                │
│                                                                      │
│  Raw Data Tables:                                                    │
│    ✓ raw_weather_data (142,372 records)                             │
│    ✓ locations (27 records)                                         │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    SPARK MLLIB (Task 3)                              │
├─────────────────────────────────────────────────────────────────────┤
│  Training: train_et_model.py                                         │
│  Prediction: predict_et_model.py                                     │
│  Model: Linear Regression (80/20 split)                              │
│  Features: precipitation_hours, sunshine, wind_speed                 │
│  Target: et0_fao_evapotranspiration                                  │
│                                                                      │
│  Output Tables:                                                      │
│    ✓ ml_feature_statistics (48 records)                             │
│    ✓ ml_model_performance (1+ records)                              │
│                                                                      │
│  Saved Model: src/spark_mllib/model/et_prediction_model/            │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│              PLOTLY DASH VISUALIZATION DASHBOARD                     │
├─────────────────────────────────────────────────────────────────────┤
│  Page 1: Precipitation Analysis (district_monthly_weather)           │
│  Page 2: Top 5 Districts (district_monthly_weather)                  │
│  Page 3: Temperature & Radiation (top_temperate_cities,              │
│          radiation_analysis, weekly_max_temp_hottest_months)         │
│  Page 4: Extreme Weather (raw_weather_data, locations)               │
│                                                                      │
│  URL: http://localhost:8050                                          │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Summary Table

| Task | Framework | Analysis | ClickHouse Table(s) | Records | Source Files |
|------|-----------|----------|---------------------|---------|--------------|
| **2.1a** | Hadoop MapReduce | District monthly precipitation & temperature | `district_monthly_weather` | 4,698 | `DistrictMonthlyWeather.java` |
| **2.1b** | Hadoop MapReduce | Highest precipitation month | `highest_precipitation` | 1 | `HighestPrecipitationMonth.java` |
| **2.2a** | Hive | Top 10 temperate cities | `top_temperate_cities` | 10 | `02_query1_top_cities.hql` |
| **2.2b** | Hive | Seasonal evapotranspiration | `evapotranspiration_by_season` | 783 | `03_query2_evapotranspiration.hql` |
| **2.3a** | Spark | Radiation > 15 MJ/m² percentage | `radiation_analysis` | 180 | `weather_spark_analysis_new.py` |
| **2.3b** | Spark | Weekly max temp (hottest months) | `weekly_max_temp_hottest_months` | 2,700+ | `weather_spark_analysis_new.py` |
| **3** | Spark MLlib | ML feature statistics | `ml_feature_statistics` | 48 | `train_et_model.py` |
| **3** | Spark MLlib | ML model performance | `ml_model_performance` | 1+ | `train_et_model.py` |
| **Support** | Dashboard | Raw daily weather data | `raw_weather_data` | 142,372 | `load_raw_data.py` |
| **Support** | Dashboard | Location/district info | `locations` | 27 | `load_raw_data.py` |

**Total Records:** 150,822 across 10 tables

---

## Verification Commands

### Check all tables exist:
```bash
docker exec clickhouse clickhouse-client --query "SHOW TABLES FROM weather_analytics"
```

### Count records per table:
```bash
# MapReduce tables
docker exec clickhouse clickhouse-client --query "SELECT count() FROM weather_analytics.district_monthly_weather"
docker exec clickhouse clickhouse-client --query "SELECT count() FROM weather_analytics.highest_precipitation"

# Hive tables
docker exec clickhouse clickhouse-client --query "SELECT count() FROM weather_analytics.top_temperate_cities"
docker exec clickhouse clickhouse-client --query "SELECT count() FROM weather_analytics.evapotranspiration_by_season"

# Spark tables
docker exec clickhouse clickhouse-client --query "SELECT count() FROM weather_analytics.radiation_analysis"
docker exec clickhouse clickhouse-client --query "SELECT count() FROM weather_analytics.weekly_max_temp_hottest_months"

# ML tables
docker exec clickhouse clickhouse-client --query "SELECT count() FROM weather_analytics.ml_feature_statistics"
docker exec clickhouse clickhouse-client --query "SELECT count() FROM weather_analytics.ml_model_performance"

# Raw data
docker exec clickhouse clickhouse-client --query "SELECT count() FROM weather_analytics.raw_weather_data"
docker exec clickhouse clickhouse-client --query "SELECT count() FROM weather_analytics.locations"
```

### Sample query - View highest precipitation:
```bash
docker exec clickhouse clickhouse-client --query "SELECT * FROM weather_analytics.highest_precipitation FORMAT Pretty"
```

### Sample query - View ML model performance:
```bash
docker exec clickhouse clickhouse-client --query "SELECT * FROM weather_analytics.ml_model_performance FORMAT Pretty"
```

---

**Document Created:** 2024-12-01
**Last Updated:** 2024-12-01
**Project Path:** `f:\MSC BIG DATA\sep sem\big data processing\assignment\project\`
