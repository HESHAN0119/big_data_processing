-- Requirement 4: Total number of days with extreme weather events
-- (high precipitation and high wind gusts)

-- NOTE: This requires raw daily weather data with wind_gusts_10m_max field
-- The district_monthly_weather table does not have wind data
-- We need to query from raw_weather_data table

-- Query 4a: Extreme weather days by district (from raw data)
SELECT
    l.city_name as district,
    COUNT(*) as extreme_weather_days,
    ROUND(AVG(w.precipitation_sum), 2) as avg_precipitation_mm,
    ROUND(AVG(w.wind_gusts_10m_max), 2) as avg_wind_gust_kmh,
    ROUND(MAX(w.precipitation_sum), 2) as max_precipitation_mm,
    ROUND(MAX(w.wind_gusts_10m_max), 2) as max_wind_gust_kmh,
    MIN(w.date) as first_event,
    MAX(w.date) as last_event
FROM weather_analytics.raw_weather_data w
LEFT JOIN weather_analytics.locations l ON w.location_id = l.location_id
WHERE w.precipitation_sum > 30  -- High precipitation threshold (30mm per day)
  AND w.wind_gusts_10m_max > 50  -- High wind gusts (50 km/h)
GROUP BY l.city_name
ORDER BY extreme_weather_days DESC;

-- Query 4b: Extreme weather by year
SELECT
    toYear(w.date) as year,
    COUNT(*) as extreme_weather_days,
    COUNT(DISTINCT l.city_name) as affected_districts,
    ROUND(AVG(w.precipitation_sum), 2) as avg_precipitation_mm,
    ROUND(AVG(w.wind_gusts_10m_max), 2) as avg_wind_gust_kmh
FROM weather_analytics.raw_weather_data w
LEFT JOIN weather_analytics.locations l ON w.location_id = l.location_id
WHERE w.precipitation_sum > 30
  AND w.wind_gusts_10m_max > 50
GROUP BY year
ORDER BY year;

-- Query 4c: Extreme weather by month (seasonality)
SELECT
    toMonth(w.date) as month,
    COUNT(*) as extreme_weather_days,
    ROUND(AVG(w.precipitation_sum), 2) as avg_precipitation_mm,
    ROUND(AVG(w.wind_gusts_10m_max), 2) as avg_wind_gust_kmh
FROM weather_analytics.raw_weather_data w
WHERE w.precipitation_sum > 30
  AND w.wind_gusts_10m_max > 50
GROUP BY month
ORDER BY month;

-- Query 4d: Scatter plot data (precipitation vs wind gusts)
SELECT
    l.city_name as district,
    w.date,
    w.precipitation_sum,
    w.wind_gusts_10m_max,
    w.temperature_2m_max,
    CASE
        WHEN w.precipitation_sum > 50 AND w.wind_gusts_10m_max > 70 THEN 'Severe'
        WHEN w.precipitation_sum > 30 AND w.wind_gusts_10m_max > 50 THEN 'Moderate'
        ELSE 'Normal'
    END as severity
FROM weather_analytics.raw_weather_data w
LEFT JOIN weather_analytics.locations l ON w.location_id = l.location_id
WHERE w.precipitation_sum > 20 OR w.wind_gusts_10m_max > 40
ORDER BY w.date DESC;

-- Query 4e: Timeline of extreme events
SELECT
    w.date,
    l.city_name as district,
    w.precipitation_sum,
    w.wind_gusts_10m_max,
    w.temperature_2m_max
FROM weather_analytics.raw_weather_data w
LEFT JOIN weather_analytics.locations l ON w.location_id = l.location_id
WHERE w.precipitation_sum > 30
  AND w.wind_gusts_10m_max > 50
ORDER BY w.date;
