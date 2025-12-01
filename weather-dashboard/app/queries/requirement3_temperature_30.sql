-- Requirement 3: Percentage of months that had a mean temperature above 30°C in a single year

-- Query 3a: Percentage by district and year
SELECT
    district,
    year,
    COUNT(*) as total_months,
    SUM(CASE WHEN mean_temperature > 30 THEN 1 ELSE 0 END) as hot_months,
    ROUND(SUM(CASE WHEN mean_temperature > 30 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as hot_month_percentage,
    ROUND(AVG(mean_temperature), 2) as avg_temp,
    ROUND(MAX(mean_temperature), 2) as max_temp
FROM weather_analytics.district_monthly_weather
GROUP BY district, year
ORDER BY district, year;

-- Query 3b: Overall statistics by district
SELECT
    district,
    COUNT(*) as total_months,
    SUM(CASE WHEN mean_temperature > 30 THEN 1 ELSE 0 END) as hot_months,
    ROUND(SUM(CASE WHEN mean_temperature > 30 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as hot_month_percentage,
    ROUND(AVG(mean_temperature), 2) as avg_temp,
    ROUND(MIN(mean_temperature), 2) as min_temp,
    ROUND(MAX(mean_temperature), 2) as max_temp
FROM weather_analytics.district_monthly_weather
GROUP BY district
ORDER BY hot_month_percentage DESC;

-- Query 3c: Year-over-year trend
SELECT
    year,
    COUNT(DISTINCT district) as districts_count,
    ROUND(AVG(CASE WHEN mean_temperature > 30 THEN 100.0 ELSE 0.0 END), 2) as avg_hot_percentage,
    SUM(CASE WHEN mean_temperature > 30 THEN 1 ELSE 0 END) as total_hot_months,
    COUNT(*) as total_months,
    ROUND(AVG(mean_temperature), 2) as avg_temp,
    ROUND(MAX(mean_temperature), 2) as max_temp_recorded
FROM weather_analytics.district_monthly_weather
GROUP BY year
ORDER BY year;

-- Query 3d: Heatmap data (district x year)
SELECT
    district,
    year,
    ROUND(SUM(CASE WHEN mean_temperature > 30 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as percentage
FROM weather_analytics.district_monthly_weather
GROUP BY district, year
ORDER BY district, year;
