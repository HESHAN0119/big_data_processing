# Zeppelin Notebook for Spark Analysis - Task 2.3

Copy each section below into separate Zeppelin notebook paragraphs.

---

## Paragraph 1: Load Data

```python
%pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Load weather data
weather_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/data/weatherData.csv")

# Load location data
location_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/data/locationData.csv")

# Parse date and add components
weather_df = weather_df.withColumn(
    "date",
    from_unixtime(unix_timestamp(col("date"), "M/d/yyyy"))
).withColumn(
    "year", year(col("date"))
).withColumn(
    "month", month(col("date"))
).withColumn(
    "week", weekofyear(col("date"))
)

# Join datasets
weather_with_location = weather_df.join(location_df, "location_id")

print("Data loaded successfully!")
print(f"Total records: {weather_with_location.count()}")
weather_with_location.printSchema()
```

---

## Paragraph 2: Task 2.3a - Shortwave Radiation Analysis

```python
%pyspark

# Calculate percentage of days with shortwave radiation > 15MJ/m² per month

radiation_analysis = weather_with_location.groupBy("year", "month").agg(
    count("*").alias("total_days"),
    sum(when(col("`shortwave_radiation_sum (MJ/m²)`") > 15, 1).otherwise(0)).alias("days_above_15"),
    avg("`shortwave_radiation_sum (MJ/m²)`").alias("avg_radiation")
)

# Calculate percentage
radiation_result = radiation_analysis.withColumn(
    "percentage_above_15",
    round((col("days_above_15") / col("total_days")) * 100, 2)
).select(
    "year",
    "month",
    "total_days",
    "days_above_15",
    "percentage_above_15",
    round("avg_radiation", 2).alias("avg_radiation")
).orderBy("year", "month")

# Display results
z.show(radiation_result.limit(50))

# Show specific example
print("\nExample: January 2010")
jan_2010 = radiation_result.filter((col("year") == 2010) & (col("month") == 1)).first()
if jan_2010:
    print(f"Percentage of days with radiation > 15MJ/m²: {jan_2010['percentage_above_15']}%")
    print(f"Average radiation: {jan_2010['avg_radiation']} MJ/m²")
```

---

## Paragraph 3: Visualize Radiation Analysis

```python
%pyspark

# Create visualization data
viz_data = radiation_result.filter(col("year") == 2020).select(
    "month",
    "percentage_above_15"
).toPandas()

print(viz_data)
```

---

## Paragraph 4: Task 2.3b - Weekly Max Temperatures for Hottest Months

```python
%pyspark

# Step 1: Find hottest months per year (top 3 months with highest avg max temp)
monthly_avg_temp = weather_with_location.groupBy("year", "month").agg(
    avg("`temperature_2m_max (°C)`").alias("avg_max_temp")
)

window_spec = Window.partitionBy("year").orderBy(desc("avg_max_temp"))
hottest_months = monthly_avg_temp.withColumn(
    "rank",
    row_number().over(window_spec)
).filter(col("rank") <= 3).select("year", "month")

print("Hottest months per year:")
z.show(hottest_months.orderBy("year", "month"))
```

---

## Paragraph 5: Calculate Weekly Maximums

```python
%pyspark

# Join weather data with hottest months
weather_hottest = weather_with_location.join(
    hottest_months,
    ["year", "month"],
    "inner"
)

# Calculate weekly max temperatures
weekly_max_temp = weather_hottest.groupBy(
    "year",
    "month",
    "week",
    "city_name"
).agg(
    max("`temperature_2m_max (°C)`").alias("weekly_max_temp"),
    avg("`temperature_2m_max (°C)`").alias("weekly_avg_temp"),
    count("*").alias("days_in_week")
)

weekly_result = weekly_max_temp.select(
    "year",
    "month",
    "week",
    "city_name",
    round("weekly_max_temp", 2).alias("max_temp"),
    round("weekly_avg_temp", 2).alias("avg_temp"),
    "days_in_week"
).orderBy("year", "month", "week", "city_name")

print("Weekly maximum temperatures for hottest months:")
z.show(weekly_result.limit(50))
```

---

## Paragraph 6: Summary Statistics by City

```python
%pyspark

# Summary statistics
summary = weekly_result.groupBy("city_name").agg(
    max("max_temp").alias("highest_recorded_temp"),
    avg("max_temp").alias("avg_max_temp_hottest_weeks"),
    count("*").alias("total_weeks_analyzed")
).orderBy(desc("highest_recorded_temp"))

print("Summary: Hottest cities during hottest months")
z.show(summary)
```

---

## Paragraph 7: Export Results (Optional)

```python
%pyspark

# Save results to CSV for dashboard
radiation_result.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .csv("/data/output/radiation_analysis")

weekly_result.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .csv("/data/output/weekly_max_temp")

print("Results exported successfully!")
```

---

## Notes for Zeppelin:

1. **Import notebook**: Create a new notebook in Zeppelin or copy each paragraph above
2. **Column names**: The CSV has spaces in column names, so use backticks like: `` `temperature_2m_max (°C)` ``
3. **Visualization**: Use Zeppelin's built-in charts by clicking the chart icons after running cells
4. **File paths**: Ensure `/data/` folder is mounted correctly in your docker-compose
