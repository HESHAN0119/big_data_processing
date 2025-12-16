
-- Query 1: Top 10 Most Temperate Cities
-- Ranks cities by average maximum temperature (lowest = most temperate)


USE weather_analytics;
    INSERT OVERWRITE DIRECTORY '{hdfs_output1}'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
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
