-- ============================================
-- Query 2: Average Evapotranspiration by Agricultural Season
-- Agricultural Seasons in Sri Lanka:
--   - Season 1 (Maha): September to March (months 9,10,11,12,1,2,3)
--   - Season 2 (Yala): April to August (months 4,5,6,7,8)
-- ============================================

USE weather_analytics;

-- This query will be executed by Python script with dynamic timestamp
-- Output format: city_name, season, year, avg_evapotranspiration
-- Output will be saved to: /user/data/hive_output/avg_evapotranspiration_by_season_<timestamp>/

SELECT
    l.city_name,
    CASE
        WHEN month_num IN (9, 10, 11, 12, 1, 2, 3) THEN 'Maha (Sep-Mar)'
        WHEN month_num IN (4, 5, 6, 7, 8) THEN 'Yala (Apr-Aug)'
    END as season,
    year_num as year,
    ROUND(AVG(w.et0_fao_evapotranspiration), 2) as avg_evapotranspiration
FROM (
    SELECT
        location_id,
        et0_fao_evapotranspiration,
        -- Extract month and year from date (format: M/D/YYYY or MM/DD/YYYY)
        CAST(SPLIT(dt, '/')[0] AS INT) as month_num,
        CAST(SPLIT(dt, '/')[2] AS INT) as year_num
    FROM weather_data
    WHERE et0_fao_evapotranspiration IS NOT NULL
) w
JOIN location_data l ON w.location_id = l.location_id
GROUP BY l.city_name, season, year_num
ORDER BY l.city_name, year_num, season;
