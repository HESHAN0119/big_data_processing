docker exec -it hive-server beeline -u "jdbc:hive2://localhost:10000"

USE weather_analytics;

SELECT
l.city_name,
ROUND(AVG(w.temperature_2m_max), 2) AS avg_max_temp,
ABS(AVG(w.temperature_2m_max) - 22) AS temp_deviation
FROM weather_data w
JOIN location_data l
ON w.location_id = l.location_id
WHERE w.temperature_2m_max IS NOT NULL
GROUP BY l.city_name
ORDER BY temp_deviation ASC
LIMIT 10;

!quit
