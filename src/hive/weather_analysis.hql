-- ========================================================================
-- Hive Analysis for Weather Data
-- ========================================================================

-- Create location table
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
LOCATION '/data/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Load location data
LOAD DATA LOCAL INPATH '/data/locationData.csv' INTO TABLE location_data;

-- Create weather table
DROP TABLE IF EXISTS weather_data;
CREATE EXTERNAL TABLE weather_data (
    location_id INT,
    date_str STRING,
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
    wind_direction_10m_dominant INT,
    shortwave_radiation_sum DOUBLE,
    et0_fao_evapotranspiration DOUBLE,
    sunrise STRING,
    sunset STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Load weather data
LOAD DATA LOCAL INPATH '/data/weatherData.csv' INTO TABLE weather_data;


-- ========================================================================
-- Task 2.2a: Rank the top 10 most temperate cities across the dataset
-- (using temperature_2m_max (°C))
-- ========================================================================

-- A "temperate" city has moderate temperatures (not too hot, not too cold)
-- We'll interpret this as cities with the LOWEST average maximum temperature
-- (cooler cities are more temperate in tropical Sri Lanka)

SELECT
    l.city_name,
    AVG(w.temperature_2m_max) AS avg_max_temperature,
    MIN(w.temperature_2m_max) AS min_max_temperature,
    MAX(w.temperature_2m_max) AS max_max_temperature,
    STDDEV(w.temperature_2m_max) AS stddev_temperature
FROM weather_data w
JOIN location_data l ON w.location_id = l.location_id
GROUP BY l.city_name
ORDER BY avg_max_temperature ASC
LIMIT 10;

-- Expected output format:
-- city_name    avg_max_temperature    min_max_temperature    max_max_temperature    stddev_temperature
-- Nuwara Eliya    20.5                15.2                   25.3                   2.1
-- Bandarawela     22.3                17.8                   27.1                   1.9
-- ...


-- ========================================================================
-- Task 2.2b: Calculate the average evapotranspiration for each major
-- agricultural season in each district over the years
-- ========================================================================

-- Agricultural seasons:
-- Yala Season (Dry): April to August (months 4-8)
-- Maha Season (Wet): September to March (months 9-12, 1-3)

SELECT
    l.city_name AS district,
    YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(w.date_str, 'M/d/yyyy'))) AS year,
    CASE
        WHEN MONTH(FROM_UNIXTIME(UNIX_TIMESTAMP(w.date_str, 'M/d/yyyy'))) BETWEEN 4 AND 8
        THEN 'Yala (April-August)'
        ELSE 'Maha (September-March)'
    END AS agricultural_season,
    AVG(w.et0_fao_evapotranspiration) AS avg_evapotranspiration,
    COUNT(*) AS num_days
FROM weather_data w
JOIN location_data l ON w.location_id = l.location_id
GROUP BY
    l.city_name,
    YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(w.date_str, 'M/d/yyyy'))),
    CASE
        WHEN MONTH(FROM_UNIXTIME(UNIX_TIMESTAMP(w.date_str, 'M/d/yyyy'))) BETWEEN 4 AND 8
        THEN 'Yala (April-August)'
        ELSE 'Maha (September-March)'
    END
ORDER BY district, year, agricultural_season;

-- Expected output format:
-- district    year    agricultural_season       avg_evapotranspiration    num_days
-- Colombo     2010    Maha (September-March)    4.2                       181
-- Colombo     2010    Yala (April-August)       5.1                       153
-- Colombo     2011    Maha (September-March)    4.3                       181
-- ...
