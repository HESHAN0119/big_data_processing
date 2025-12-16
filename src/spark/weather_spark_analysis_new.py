#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys


try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, year, month, weekofyear, when, count,
        sum as spark_sum, max as spark_max, avg,
        round as spark_round, from_unixtime, unix_timestamp, desc, row_number
    )
    from pyspark.sql.types import *
    from pyspark.sql.window import Window
except ImportError:
    print("ERROR: PySpark not found.")
    print("Install PySpark: pip install pyspark")
    sys.exit(1)

try:
    import requests
except ImportError:
    print("ERROR: requests library not found.")
    print("Install it with: pip install requests")
    sys.exit(1)

# ========================================================================
# CONFIGURATION
# ========================================================================

# ClickHouse Configuration
CLICKHOUSE_HOST = 'clickhouse'  # Docker service name
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DB = 'weather_analytics'

# HDFS Paths (Kafka Ingested Data)
# Note: These paths contain CSV files written by kafka_hdfs_consumer.py
HDFS_BASE = "hdfs://namenode:9000"
LOCATION_PATH = f"{HDFS_BASE}/user/data/kafka_ingested/location/"
WEATHER_PATH = f"{HDFS_BASE}/user/data/kafka_ingested/weather/"

# Spark/YARN Configuration
SPARK_APP_NAME = "Weather Analysis - Kafka to ClickHouse"
SPARK_MASTER = "spark://spark-master:7077"  # Use Spark standalone cluster
SPARK_DEPLOY_MODE = "client"  # Client mode for interactive output


# ------------------------- data insert CLICKHOUSE FUNCTIONS


def execute_clickhouse_query(query, verbose=True):
    """Execute SQL query on ClickHouse via HTTP API"""
    url = f'http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}'
    params = {'database': CLICKHOUSE_DB}

    try:
        response = requests.post(url, params=params, data=query.encode('utf-8'), timeout=60)
        response.raise_for_status()
        if verbose:
            print(f"✓ Query executed")
        return response.text
    except Exception as e:
        print(f"✗ Error: {e}")
        return None

def create_clickhouse_tables():
    """Create ClickHouse tables if they don't exist"""
    print("\n" + "="*80)
    print("CREATING CLICKHOUSE TABLES")
    print("="*80)

    # Table 1: Radiation Analysis Results
    table1 = """
    CREATE TABLE IF NOT EXISTS radiation_analysis (
        year UInt16,
        month UInt8,
        total_days UInt32,
        days_above_15 UInt32,
        percentage_above_15 Float64,
        avg_radiation Float64,
        created_at DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY (year, month)
    """
    print("\n[1/2] Creating: radiation_analysis")
    execute_clickhouse_query(table1)

    # Table 2: Weekly Max Temperature (Hottest Months)
    table2 = """
    CREATE TABLE IF NOT EXISTS weekly_max_temp_hottest_months (
        year UInt16,
        month UInt8,
        week UInt8,
        city_name String,
        weekly_max_temp Float64,
        weekly_avg_temp Float64,
        days_in_week UInt8,
        created_at DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY (year, month, week, city_name)
    """
    print("[2/2] Creating: weekly_max_temp_hottest_months")
    execute_clickhouse_query(table2)

    print("\n✓ Tables created successfully\n")

def write_to_clickhouse(df, table_name, mode="append"):

    print(f"\n{'='*80}")
    print(f"WRITING TO CLICKHOUSE: {table_name}")
    print(f"{'='*80}")

    # Collect data (assumes result sets are small enough for driver memory)
    data = df.collect()
    row_count = len(data)

    if row_count == 0:
        print("No data to write")
        return False

    print(f"Collected {row_count} rows from Spark")

    # Truncate table if overwrite mode
    if mode == "overwrite":
        print(f"Truncating table {table_name}...")
        execute_clickhouse_query(f"TRUNCATE TABLE {table_name}", verbose=False)

    # Prepare batch insert
    columns = df.columns
    batch_size = 1000
    total_batches = (row_count - 1) // batch_size + 1

    print(f"Inserting in {total_batches} batch(es) of {batch_size} rows...")

    # Build and execute INSERT queries
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, row_count)
        batch_data = data[start_idx:end_idx]

        # Build VALUES clause
        values_list = []
        for row in batch_data:
            values = []
            for col_name in columns:
                val = row[col_name]
                if val is None:
                    values.append("NULL")
                elif isinstance(val, str):
                    # Escape quotes
                    escaped = val.replace("'", "\\'")
                    values.append(f"'{escaped}'")
                else:
                    values.append(str(val))
            values_list.append(f"({', '.join(values)})")

        # Execute INSERT
        insert_query = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES {', '.join(values_list)}
        """

        result = execute_clickhouse_query(insert_query, verbose=False)
        if result is None:
            print(f"✗ Batch {batch_num + 1}/{total_batches} failed")
            return False

        print(f"✓ Batch {batch_num + 1}/{total_batches} inserted ({len(batch_data)} rows)")

    # Verify final count
    count_query = f"SELECT count() FROM {table_name}"
    final_count = execute_clickhouse_query(count_query, verbose=False)
    print(f"\n✓ Successfully wrote {row_count} rows")
    print(f"✓ Total rows in {table_name}: {final_count.strip()}\n")

    return True

# MAIN ANALYSIS PIPELINE

def main():
    """Main analysis pipeline"""

    print("-"*80)
    print("SPARK WEATHER ANALYSIS - TASK 2.3")
    print("Kafka Ingested Data → ClickHouse")
    print("-"*80)


    # -------------Initialize Spark Session


    print("\n[STEP 1] Initializing Spark Session with YARN")

    spark = SparkSession.builder \
        .appName(SPARK_APP_NAME) \
        .master(SPARK_MASTER) \
        .config("spark.submit.deployMode", SPARK_DEPLOY_MODE) \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()

    print(f"✓ Spark initialized")
    print(f"  Version: {spark.version}")
    print(f"  Master: {spark.sparkContext.master}")
    print(f"  App ID: {spark.sparkContext.applicationId}")

    
    # Create ClickHouse Tables
    

    print("\n[STEP 2] Setting up ClickHouse tables...")
    create_clickhouse_tables()

    
    #  Load Data from HDFS
    

    print("\n[STEP 3] Loading data from HDFS (Kafka ingested)...")

    # Location data schema (from locationData_3.csv)
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

    # Weather data schema (from weatherData.csv)
    weather_schema = StructType([
        StructField("location_id", IntegerType(), True),
        StructField("date", StringType(), True),  # Format: M/D/YYYY
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
        StructField("wind_direction_10m_dominant", DoubleType(), True),
        StructField("shortwave_radiation_sum", DoubleType(), True),
        StructField("et0_fao_evapotranspiration", DoubleType(), True),
        StructField("sunrise", StringType(), True),
        StructField("sunset", StringType(), True)
    ])

    # Load location data
    print(f"\nLoading location data from: {LOCATION_PATH}")
    location_df = spark.read \
        .option("header", "true") \
        .schema(location_schema) \
        .csv(LOCATION_PATH)

    location_count = location_df.count()
    print(f"✓ Loaded {location_count} locations")

    # Load weather data
    print(f"\nLoading weather data from: {WEATHER_PATH}")
    weather_df = spark.read \
        .option("header", "true") \
        .schema(weather_schema) \
        .csv(WEATHER_PATH)

    weather_count = weather_df.count()
    print(f"✓ Loaded {weather_count:,} weather records")

    # Parse dates and extract time components
    print("\nParsing dates and adding time components...")
    weather_df = weather_df.withColumn(
        "date_parsed",
        from_unixtime(unix_timestamp(col("date"), "M/d/yyyy"))
    ).withColumn(
        "year", year(col("date_parsed"))
    ).withColumn(
        "month", month(col("date_parsed"))
    ).withColumn(
        "week", weekofyear(col("date_parsed"))
    )

    # Join weather + location data
    print("Joining weather and location data...")
    weather_with_location = weather_df.join(location_df, "location_id", "inner")

    joined_count = weather_with_location.count()
    print(f"✓ Joined dataset: {joined_count:,} records")

    # Show sample
    print("\nSample data:")
    weather_with_location.select(
        "city_name", "date", "temperature_2m_max", "shortwave_radiation_sum"
    ).show(5, truncate=False)

    
    # -----Task 2.3a - Radiation Analysis
    

    print("\n[STEP 4] TASK 2.3a: Radiation Analysis")
    print("-"*80)
    print("Calculate percentage of radiation > 15MJ/m2 per month")
    print("-"*80)

    # Group by year, month and calculate statistics
    radiation_stats = weather_with_location.groupBy("year", "month").agg(
        count("*").alias("total_days"),
        spark_sum(
            when(col("shortwave_radiation_sum") > 15, 1).otherwise(0)
        ).alias("days_above_15"),
        avg("shortwave_radiation_sum").alias("avg_radiation")
    )

    # Calculate percentage
    radiation_analysis = radiation_stats.withColumn(
        "percentage_above_15",
        spark_round((col("days_above_15") / col("total_days")) * 100, 2)
    )

    # Select final columns
    radiation_result = radiation_analysis.select(
        col("year").cast("int"),
        col("month").cast("int"),
        col("total_days").cast("int"),
        col("days_above_15").cast("int"),
        col("percentage_above_15").cast("double"),
        spark_round("avg_radiation", 2).alias("avg_radiation").cast("double")
    ).orderBy("year", "month")

    print("\nResults (sample):")
    radiation_result.show(10, truncate=False)

    # Write to ClickHouse
    write_to_clickhouse(radiation_result, "radiation_analysis", mode="overwrite")

    
    # STEP 5: Task 2.3b - Weekly Max Temperature (Hottest Months)
    

    print("\n[STEP 5] TASK 2.3b: Weekly Max Temperature (Hottest Months)")
    print("="*80)
    print("Analyze weekly max temps for the hottest months of each year")
    print("="*80)

    # Step 5.1: Find hottest months per year
    print("\nStep 5.1: Identifying hottest months per year...")

    monthly_avg_temp = weather_with_location.groupBy("year", "month").agg(
        avg("temperature_2m_max").alias("avg_max_temp")
    )

    # -----------------------Rank months within each year (top 3)
    window_spec = Window.partitionBy("year").orderBy(desc("avg_max_temp"))
    hottest_months = monthly_avg_temp.withColumn(
        "rank",
        row_number().over(window_spec)
    ).filter(col("rank") <= 3).select("year", "month")

    print("Hottest months (top 3 per year):")
    hottest_months.orderBy("year", "month").show(15)

    # Step 5.2: Filter weather data for hottest months
    print("\nStep 5.2: Filtering for hottest months...")
    weather_hottest = weather_with_location.join(
        hottest_months,
        ["year", "month"],
        "inner"
    )

    hottest_filtered_count = weather_hottest.count()
    print(f"✓ Filtered to {hottest_filtered_count:,} records")

    # Step 5.3: Calculate weekly max temperatures
    print("\nStep 5.3: Calculating weekly statistics...")
    weekly_stats = weather_hottest.groupBy(
        "year", "month", "week", "city_name"
    ).agg(
        spark_max("temperature_2m_max").alias("weekly_max_temp"),
        avg("temperature_2m_max").alias("weekly_avg_temp"),
        count("*").alias("days_in_week")
    )

    # Prepare final result
    weekly_result = weekly_stats.select(
        col("year").cast("int"),
        col("month").cast("int"),
        col("week").cast("int"),
        col("city_name"),
        spark_round("weekly_max_temp", 2).alias("weekly_max_temp").cast("double"),
        spark_round("weekly_avg_temp", 2).alias("weekly_avg_temp").cast("double"),
        col("days_in_week").cast("int")
    ).orderBy("year", "month", "week", "city_name")

    print("\nResults (sample):")
    weekly_result.show(20, truncate=False)

    # Write to ClickHouse
    write_to_clickhouse(weekly_result, "weekly_max_temp_hottest_months", mode="overwrite")

    
    # STEP 6: Summary
    

    print("\n" + "="*80)
    print("ANALYSIS COMPLETE!")
    print("="*80)

    radiation_count = radiation_result.count()
    weekly_count = weekly_result.count()

    print(f"\n✓ Task 2.3a: Radiation Analysis")
    print(f"  Records written: {radiation_count}")
    print(f"  Table: radiation_analysis")

    print(f"\n✓ Task 2.3b: Weekly Max Temperature (Hottest Months)")
    print(f"  Records written: {weekly_count}")
    print(f"  Table: weekly_max_temp_hottest_months")

    print(f"\n{'='*80}")
    print("CLICKHOUSE ACCESS")
    print("="*80)
    print(f"Web UI: http://localhost:8123/play")
    print(f"Database: {CLICKHOUSE_DB}")
    print(f"\nSample Queries:")
    print(f"  SELECT * FROM radiation_analysis ORDER BY year, month;")
    print(f"  SELECT * FROM weekly_max_temp_hottest_months ORDER BY year, month, week;")

    # Stop Spark
    spark.stop()
    print("\n✓ Spark session stopped")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n✗ Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
