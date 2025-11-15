-- ============================================
-- Query 1: Top 10 Most Temperate Cities
-- Ranks cities by average maximum temperature (lowest = most temperate)
-- ============================================

USE weather_analytics;

-- This query will be executed by Python script with dynamic timestamp
-- Output format: city_name, avg_max_temp
-- Output will be saved to: /user/data/hive_output/top_10_temperate_cities_<timestamp>/

SELECT
    l.city_name,
    ROUND(AVG(w.temperature_2m_max), 2) as avg_max_temp
FROM weather_data w
JOIN location_data l ON w.location_id = l.location_id
WHERE w.temperature_2m_max IS NOT NULL
GROUP BY l.city_name
ORDER BY avg_max_temp ASC
LIMIT 10;
