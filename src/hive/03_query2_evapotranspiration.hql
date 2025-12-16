-- ============================================
-- Query 2: Average Evapotranspiration by Agricultural Season
-- Agricultural Seasons in Sri Lanka:
--   - Season 1 (Maha): September to March (months 9,10,11,12,1,2,3)
--   - Season 2 (Yala): April to August (months 4,5,6,7,8)
-- ============================================

USE weather_analytics;

INSERT OVERWRITE DIRECTORY '{hdfs_output2}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','

SELECT
    l.city_name,
    CASE
        WHEN CAST(split(w.dt, '/')[0] AS INT) IN (1,2,3,9,10,11,12) THEN 'Maha'
        ELSE 'Yala'
    END AS season,
    CASE
    WHEN CAST(split(w.dt, '/')[0] AS INT) IN (1,2,3) THEN CAST(split(w.dt, '/')[2] AS INT) - 1
    ELSE CAST(split(w.dt, '/')[2] AS INT)
END AS year,
    ROUND(AVG(w.et0_fao_evapotranspiration), 2) AS avg_evapotranspiration
FROM weather_data w
JOIN location_data l ON w.location_id = l.location_id
WHERE w.et0_fao_evapotranspiration IS NOT NULL
  AND w.dt IS NOT NULL
  AND w.dt RLIKE '^[0-9]+/[0-9]+/[0-9]+$'
GROUP BY
    l.city_name,
    CASE
    WHEN CAST(split(w.dt, '/')[0] AS INT) IN (1,2,3) THEN CAST(split(w.dt, '/')[2] AS INT) - 1
    ELSE CAST(split(w.dt, '/')[2] AS INT)
    END,
    CASE
        WHEN CAST(split(w.dt, '/')[0] AS INT) IN (1,2,3,9,10,11,12) THEN 'Maha'
        ELSE 'Yala'
    END
ORDER BY l.city_name, year, season;