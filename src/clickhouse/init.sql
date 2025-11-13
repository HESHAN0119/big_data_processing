-- ClickHouse initialization script
-- Creates database and tables for weather analytics

-- Create database
CREATE DATABASE IF NOT EXISTS weather_analytics;

USE weather_analytics;

-- Table 1: District Monthly Weather Statistics
-- Stores the results from DistrictMonthlyWeather MapReduce job
CREATE TABLE IF NOT EXISTS district_monthly_weather (
    district String,
    year UInt16,
    month UInt8,
    year_month String,
    total_precipitation_hours Float64,
    mean_temperature Float64,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (district, year, month);

-- Table 2: Highest Precipitation Records
-- Stores the results from HighestPrecipitationMonth MapReduce job
CREATE TABLE IF NOT EXISTS highest_precipitation (
    year UInt16,
    month UInt8,
    year_month String,
    total_precipitation_hours Float64,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (year, month);

-- Table 3: Raw Weather Data (optional - for direct queries)
CREATE TABLE IF NOT EXISTS raw_weather_data (
    location_id UInt32,
    date Date,
    weather_code UInt16,
    temperature_2m_max Float64,
    temperature_2m_min Float64,
    temperature_2m_mean Float64,
    apparent_temperature_max Float64,
    apparent_temperature_min Float64,
    apparent_temperature_mean Float64,
    daylight_duration Float64,
    sunshine_duration Float64,
    precipitation_sum Float64,
    rain_sum Float64,
    precipitation_hours Float64,
    wind_speed_10m_max Float64,
    wind_gusts_10m_max Float64,
    wind_direction_10m_dominant Float64,
    shortwave_radiation_sum Float64,
    et0_fao_evapotranspiration Float64,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (location_id, date);

-- Table 4: Location Data
CREATE TABLE IF NOT EXISTS locations (
    location_id UInt32,
    latitude Float64,
    longitude Float64,
    elevation Float64,
    utc_offset_seconds Int32,
    timezone String,
    timezone_abbreviation String,
    city_name String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY location_id;

-- Create materialized view for monthly aggregations
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_monthly_summary
ENGINE = SummingMergeTree()
ORDER BY (district, year, month)
AS SELECT
    l.city_name as district,
    toYear(w.date) as year,
    toMonth(w.date) as month,
    sum(w.precipitation_hours) as total_precipitation_hours,
    avg(w.temperature_2m_mean) as avg_temperature,
    count() as record_count
FROM raw_weather_data w
LEFT JOIN locations l ON w.location_id = l.location_id
GROUP BY district, year, month;
