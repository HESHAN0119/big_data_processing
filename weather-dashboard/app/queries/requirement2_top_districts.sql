-- Requirement 2: Top 5 districts based on total amount of precipitation

-- Query 2a: Top 5 districts by total precipitation (all years)
SELECT
    district,
    ROUND(SUM(total_precipitation_hours), 2) as total_precip_hours,
    ROUND(AVG(total_precipitation_hours), 2) as avg_monthly_precip,
    ROUND(MIN(total_precipitation_hours), 2) as min_monthly_precip,
    ROUND(MAX(total_precipitation_hours), 2) as max_monthly_precip,
    COUNT(*) as month_count,
    MIN(year) as first_year,
    MAX(year) as last_year
FROM weather_analytics.district_monthly_weather
GROUP BY district
ORDER BY total_precip_hours DESC
LIMIT 5;

-- Query 2b: Top 5 districts by average monthly precipitation
SELECT
    district,
    ROUND(AVG(total_precipitation_hours), 2) as avg_monthly_precip,
    ROUND(SUM(total_precipitation_hours), 2) as total_precip_hours,
    COUNT(*) as month_count
FROM weather_analytics.district_monthly_weather
GROUP BY district
ORDER BY avg_monthly_precip DESC
LIMIT 5;

-- Query 2c: Yearly breakdown for top 5 districts
WITH top_districts AS (
    SELECT district
    FROM weather_analytics.district_monthly_weather
    GROUP BY district
    ORDER BY SUM(total_precipitation_hours) DESC
    LIMIT 5
)
SELECT
    d.district,
    d.year,
    ROUND(SUM(d.total_precipitation_hours), 2) as yearly_precip,
    ROUND(AVG(d.total_precipitation_hours), 2) as avg_monthly_precip
FROM weather_analytics.district_monthly_weather d
INNER JOIN top_districts t ON d.district = t.district
GROUP BY d.district, d.year
ORDER BY d.district, d.year;
