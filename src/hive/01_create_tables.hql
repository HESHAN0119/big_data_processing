-- ============================================
-- Hive Table Creation Script
-- Points to Kafka-ingested data in HDFS
-- ============================================

-- Step 1: Create Database
CREATE DATABASE IF NOT EXISTS weather_analytics;
USE weather_analytics;

-- Step 2: Create Location Table
-- Points to: /user/data/kafka_ingested/location/
DROP TABLE IF EXISTS location_data;
CREATE EXTERNAL TABLE location_data (
    location_id INT,
    latitude DOUBLE,
    longitude DOUBLE,
    elevation INT,
    utc_offset_seconds INT,
    timezone STRING,
    timezone_abbreviation STRING,
    city_name STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/data/kafka_ingested/location'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Step 3: Create Weather Table
-- Points to: /user/data/kafka_ingested/weather/
DROP TABLE IF EXISTS weather_data;
CREATE EXTERNAL TABLE weather_data (
    location_id INT,
    date STRING,
    weather_code INT,
    temperature_2m_max DOUBLE,
    temperature_2m_min DOUBLE,
    temperature_2m_mean DOUBLE,
    apparent_temperature_max DOUBLE,
    apparent_temperature_min DOUBLE,
    apparent_temperature_mean DOUBLE,
    daylight_duration DOUBLE,
    sunshine_duration DOUBLE,
    precipitation_sum DOUBLE,
    rain_sum DOUBLE,
    precipitation_hours DOUBLE,
    wind_speed_10m_max DOUBLE,
    wind_gusts_10m_max DOUBLE,
    wind_direction_10m_dominant DOUBLE,
    shortwave_radiation_sum DOUBLE,
    et0_fao_evapotranspiration DOUBLE,
    sunrise STRING,
    sunset STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/data/kafka_ingested/weather'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Step 4: Verify Tables
SHOW TABLES;

-- Check record counts
SELECT COUNT(*) as location_count FROM location_data;
SELECT COUNT(*) as weather_count FROM weather_data;

-- Preview data
SELECT * FROM location_data LIMIT 5;
SELECT * FROM weather_data LIMIT 5;
