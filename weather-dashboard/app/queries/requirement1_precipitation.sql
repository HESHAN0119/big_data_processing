-- Requirement 1: Most precipitous month/season for each district
-- Query 1a: Monthly precipitation by district
SELECT
    district,
    year,
    month,
    total_precipitation_hours,
    mean_temperature
FROM weather_analytics.district_monthly_weather
ORDER BY district, year, month;

-- Query 1b: Average precipitation by month (across all years) for each district
SELECT
    district,
    month,
    ROUND(AVG(total_precipitation_hours), 2) as avg_precip_hours,
    ROUND(MIN(total_precipitation_hours), 2) as min_precip_hours,
    ROUND(MAX(total_precipitation_hours), 2) as max_precip_hours,
    COUNT(*) as year_count
FROM weather_analytics.district_monthly_weather
GROUP BY district, month
ORDER BY district, month;

-- Query 1c: Seasonal precipitation (Maha vs Yala)
SELECT
    district,
    CASE
        WHEN month IN (9,10,11,12,1,2,3) THEN 'Maha (Sep-Mar)'
        WHEN month IN (4,5,6,7,8) THEN 'Yala (Apr-Aug)'
    END as season,
    year,
    ROUND(SUM(total_precipitation_hours), 2) as total_precip,
    ROUND(AVG(total_precipitation_hours), 2) as avg_precip,
    ROUND(AVG(mean_temperature), 2) as avg_temp
FROM weather_analytics.district_monthly_weather
GROUP BY district, season, year
ORDER BY district, year, season;

-- Query 1d: Most precipitous month for each district (overall)
SELECT
    district,
    month,
    ROUND(AVG(total_precipitation_hours), 2) as avg_precip_hours
FROM weather_analytics.district_monthly_weather
GROUP BY district, month
ORDER BY district, avg_precip_hours DESC;
