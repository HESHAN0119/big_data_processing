"""
Spark Analysis for Weather Data - Task 2.3
Can be run in Zeppelin or as a standalone PySpark script
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, weekofyear, date_format,
    when, count, sum as spark_sum, max as spark_max,
    from_unixtime, unix_timestamp, avg, round as spark_round
)
from pyspark.sql.types import *

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Weather Analysis") \
    .getOrCreate()

# ========================================================================
# Load Data
# ========================================================================

# Define schema for weather data
weather_schema = StructType([
    StructField("location_id", IntegerType(), True),
    StructField("date_str", StringType(), True),
    StructField("weather_code", IntegerType(), True),
    StructField("temperature_2m_max", DoubleType(), True),
    StructField("temperature_2m_min", DoubleType(), True),
    StructField("temperature_2m_mean", DoubleType(), True),
    StructField("apparent_temperature_max", DoubleType(), True),
    StructField("apparent_temperature_min", DoubleType(), True),
    StructField("apparent_temperature_mean", DoubleType(), True),
    StructField("daylight_duration", DoubleType(), True),
    StructField("sunshine_duration", DoubleType(), True),
    StructField("precipitation_sum", DoubleType(), True),
    StructField("rain_sum", DoubleType(), True),
    StructField("precipitation_hours", DoubleType(), True),
    StructField("wind_speed_10m_max", DoubleType(), True),
    StructField("wind_gusts_10m_max", DoubleType(), True),
    StructField("wind_direction_10m_dominant", IntegerType(), True),
    StructField("shortwave_radiation_sum", DoubleType(), True),
    StructField("et0_fao_evapotranspiration", DoubleType(), True),
    StructField("sunrise", StringType(), True),
    StructField("sunset", StringType(), True)
])

# Define schema for location data
location_schema = StructType([
    StructField("location_id", IntegerType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("elevation", IntegerType(), True),
    StructField("utc_offset_seconds", IntegerType(), True),
    StructField("timezone", StringType(), True),
    StructField("timezone_abbreviation", StringType(), True),
    StructField("city_name", StringType(), True)
])

# Load data
weather_df = spark.read \
    .option("header", "true") \
    .schema(weather_schema) \
    .csv("/data/weatherData.csv")

location_df = spark.read \
    .option("header", "true") \
    .schema(location_schema) \
    .csv("/data/locationData.csv")

# Parse date and add date components
weather_df = weather_df.withColumn(
    "date",
    from_unixtime(unix_timestamp(col("date_str"), "M/d/yyyy"))
).withColumn(
    "year", year(col("date"))
).withColumn(
    "month", month(col("date"))
).withColumn(
    "week", weekofyear(col("date"))
)

# Join with location data
weather_with_location = weather_df.join(location_df, "location_id")

print("Data loaded successfully!")
weather_with_location.printSchema()

# ========================================================================
# Task 2.3a: Calculate the percentage of total shortwave radiation
# which is more than 15MJ/m² in a month across all districts
# ========================================================================

print("\n" + "="*80)
print("Task 2.3a: Percentage of shortwave radiation > 15MJ/m² per month")
print("="*80)

# Group by year-month and calculate percentage
radiation_analysis = weather_with_location.groupBy("year", "month").agg(
    count("*").alias("total_days"),
    spark_sum(
        when(col("shortwave_radiation_sum") > 15, 1).otherwise(0)
    ).alias("days_above_15"),
    avg("shortwave_radiation_sum").alias("avg_radiation")
)

# Calculate percentage
radiation_analysis = radiation_analysis.withColumn(
    "percentage_above_15",
    spark_round((col("days_above_15") / col("total_days")) * 100, 2)
)

# Sort by year and month
radiation_result = radiation_analysis.select(
    "year",
    "month",
    "total_days",
    "days_above_15",
    "percentage_above_15",
    spark_round("avg_radiation", 2).alias("avg_radiation")
).orderBy("year", "month")

print("\nResults:")
radiation_result.show(20, truncate=False)

# Save results
radiation_result.coalesce(1).write.mode("overwrite").csv(
    "/output/radiation_analysis",
    header=True
)

print("\nSample interpretation:")
print("In January 2010, {:.2f}% of days had shortwave radiation > 15MJ/m²".format(
    radiation_result.filter((col("year") == 2010) & (col("month") == 1))
    .select("percentage_above_15").first()[0]
))


# ========================================================================
# Task 2.3b: Weekly maximum temperatures for the hottest months of a year
# ========================================================================

print("\n" + "="*80)
print("Task 2.3b: Weekly maximum temperatures for hottest months")
print("="*80)

# Step 1: Find the hottest months of each year
# (months with highest average maximum temperature)
monthly_avg_temp = weather_with_location.groupBy("year", "month").agg(
    avg("temperature_2m_max").alias("avg_max_temp")
)

# For each year, find the top 3 hottest months
from pyspark.sql.window import Window
import pyspark.sql.functions as F

window_spec = Window.partitionBy("year").orderBy(F.desc("avg_max_temp"))
hottest_months = monthly_avg_temp.withColumn(
    "rank",
    F.row_number().over(window_spec)
).filter(col("rank") <= 3).select("year", "month")

print("\nHottest months per year:")
hottest_months.orderBy("year", "month").show(20)

# Step 2: Get weekly max temperatures for these hottest months
weather_hottest = weather_with_location.join(
    hottest_months,
    ["year", "month"],
    "inner"
)

weekly_max_temp = weather_hottest.groupBy(
    "year",
    "month",
    "week",
    "city_name"
).agg(
    spark_max("temperature_2m_max").alias("weekly_max_temp"),
    avg("temperature_2m_max").alias("weekly_avg_temp"),
    count("*").alias("days_in_week")
)

# Sort and display results
weekly_result = weekly_max_temp.select(
    "year",
    "month",
    "week",
    "city_name",
    spark_round("weekly_max_temp", 2).alias("max_temp"),
    spark_round("weekly_avg_temp", 2).alias("avg_temp"),
    "days_in_week"
).orderBy("year", "month", "week", "city_name")

print("\nWeekly maximum temperatures for hottest months:")
weekly_result.show(30, truncate=False)

# Save results
weekly_result.coalesce(1).write.mode("overwrite").csv(
    "/output/weekly_max_temp_hottest_months",
    header=True
)

# Show summary statistics
print("\nSummary Statistics:")
weekly_result.groupBy("city_name").agg(
    spark_max("max_temp").alias("highest_recorded_temp"),
    avg("max_temp").alias("avg_max_temp_hottest_weeks"),
    count("*").alias("total_weeks_analyzed")
).orderBy(F.desc("highest_recorded_temp")).show(10, truncate=False)

print("\n" + "="*80)
print("Analysis Complete!")
print("="*80)

# Stop Spark session
# spark.stop()
