#!/usr/bin/env python3
"""
Load raw location and weather data from CSV files into ClickHouse
Replaces existing tables with fresh data
"""

import csv
import requests
from datetime import datetime
import os

# Configuration
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DB = 'weather_analytics'
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = ''

# Data file paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOCATION_DATA_PATH = os.path.join(BASE_DIR, 'data', 'locationData_3.csv')
WEATHER_DATA_PATH = os.path.join(BASE_DIR, 'data', 'weatherData.csv')


def execute_clickhouse_query(query, verbose=True):
    """Execute a query on ClickHouse"""
    url = f'http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}'
    params = {'database': CLICKHOUSE_DB}
    auth = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD) if CLICKHOUSE_USER else None

    try:
        response = requests.post(url, params=params, auth=auth, data=query.encode('utf-8'))
        response.raise_for_status()
        if verbose:
            print(f"✓ Query executed successfully")
        return response.text
    except Exception as e:
        print(f"✗ Error executing query: {e}")
        if hasattr(e, 'response') and hasattr(e.response, 'text'):
            print(f"Response: {e.response.text}")
        return None


def drop_and_create_locations_table():
    """Drop and recreate the locations table"""
    print("\n" + "="*60)
    print("Creating locations table")
    print("="*60)

    # Drop table if exists
    drop_query = "DROP TABLE IF EXISTS locations"
    execute_clickhouse_query(drop_query)
    print("✓ Dropped existing locations table")

    # Create table
    create_query = """
    CREATE TABLE locations (
        location_id UInt32,
        latitude Float64,
        longitude Float64,
        elevation Float64,
        utc_offset_seconds Int32,
        timezone String,
        timezone_abbreviation String,
        city_name String,
        created_at DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY location_id
    """
    execute_clickhouse_query(create_query)
    print("✓ Created locations table")


def drop_and_create_raw_weather_table():
    """Drop and recreate the raw_weather_data table"""
    print("\n" + "="*60)
    print("Creating raw_weather_data table")
    print("="*60)

    # Drop table if exists
    drop_query = "DROP TABLE IF EXISTS raw_weather_data"
    execute_clickhouse_query(drop_query)
    print("✓ Dropped existing raw_weather_data table")

    # Create table
    create_query = """
    CREATE TABLE raw_weather_data (
        location_id UInt32,
        date Date,
        weather_code UInt16,
        temperature_2m_max Float64,
        temperature_2m_min Float64,
        temperature_2m_mean Float64,
        apparent_temperature_max Float64,
        apparent_temperature_min Float64,
        apparent_temperature_mean Float64,
        daylight_duration Float64,
        sunshine_duration Float64,
        precipitation_sum Float64,
        rain_sum Float64,
        precipitation_hours Float64,
        wind_speed_10m_max Float64,
        wind_gusts_10m_max Float64,
        wind_direction_10m_dominant Float64,
        shortwave_radiation_sum Float64,
        et0_fao_evapotranspiration Float64,
        created_at DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY (location_id, date)
    """
    execute_clickhouse_query(create_query)
    print("✓ Created raw_weather_data table")


def load_locations_data():
    """Load location data from CSV into ClickHouse"""
    print("\n" + "="*60)
    print("Loading location data")
    print("="*60)

    if not os.path.exists(LOCATION_DATA_PATH):
        print(f"✗ Location data file not found: {LOCATION_DATA_PATH}")
        return

    print(f"Reading from: {LOCATION_DATA_PATH}")

    # Read CSV and prepare data
    rows_to_insert = []
    with open(LOCATION_DATA_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows_to_insert.append(
                f"{row['location_id']}\t"
                f"{row['latitude']}\t"
                f"{row['longitude']}\t"
                f"{row['elevation']}\t"
                f"{row['utc_offset_seconds']}\t"
                f"{row['timezone']}\t"
                f"{row['timezone_abbreviation']}\t"
                f"{row['city_name']}"
            )

    print(f"Parsed {len(rows_to_insert)} location records")

    # Insert data using TabSeparated format
    data = '\n'.join(rows_to_insert)
    url = f'http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}'
    params = {
        'database': CLICKHOUSE_DB,
        'query': 'INSERT INTO locations (location_id, latitude, longitude, elevation, utc_offset_seconds, timezone, timezone_abbreviation, city_name) FORMAT TabSeparated'
    }
    auth = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD) if CLICKHOUSE_USER else None

    response = requests.post(url, params=params, auth=auth, data=data.encode('utf-8'))
    response.raise_for_status()

    print(f"✓ Successfully loaded {len(rows_to_insert)} location records")

    # Verify
    count_query = "SELECT count() FROM locations"
    count = execute_clickhouse_query(count_query, verbose=False)
    print(f"✓ Total records in locations table: {count.strip()}")


def parse_date(date_str):
    """Parse date from M/D/YYYY format to YYYY-MM-DD"""
    parts = date_str.split('/')
    month = parts[0].zfill(2)
    day = parts[1].zfill(2)
    year = parts[2]
    return f"{year}-{month}-{day}"


def load_weather_data():
    """Load weather data from CSV into ClickHouse"""
    print("\n" + "="*60)
    print("Loading weather data")
    print("="*60)

    if not os.path.exists(WEATHER_DATA_PATH):
        print(f"✗ Weather data file not found: {WEATHER_DATA_PATH}")
        return

    print(f"Reading from: {WEATHER_DATA_PATH}")

    # Read CSV in batches for efficiency
    batch_size = 1000
    rows_to_insert = []
    total_rows = 0

    with open(WEATHER_DATA_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            # Parse date from M/D/YYYY to YYYY-MM-DD
            date_formatted = parse_date(row['date'])

            rows_to_insert.append(
                f"{row['location_id']}\t"
                f"{date_formatted}\t"
                f"{row['weather_code (wmo code)']}\t"
                f"{row['temperature_2m_max (°C)']}\t"
                f"{row['temperature_2m_min (°C)']}\t"
                f"{row['temperature_2m_mean (°C)']}\t"
                f"{row['apparent_temperature_max (°C)']}\t"
                f"{row['apparent_temperature_min (°C)']}\t"
                f"{row['apparent_temperature_mean (°C)']}\t"
                f"{row['daylight_duration (s)']}\t"
                f"{row['sunshine_duration (s)']}\t"
                f"{row['precipitation_sum (mm)']}\t"
                f"{row['rain_sum (mm)']}\t"
                f"{row['precipitation_hours (h)']}\t"
                f"{row['wind_speed_10m_max (km/h)']}\t"
                f"{row['wind_gusts_10m_max (km/h)']}\t"
                f"{row['wind_direction_10m_dominant (°)']}\t"
                f"{row['shortwave_radiation_sum (MJ/m²)']}\t"
                f"{row['et0_fao_evapotranspiration (mm)']}"
            )

            # Insert in batches
            if len(rows_to_insert) >= batch_size:
                insert_batch(rows_to_insert)
                total_rows += len(rows_to_insert)
                print(f"  Inserted {total_rows} records...", end='\r')
                rows_to_insert = []

        # Insert remaining rows
        if rows_to_insert:
            insert_batch(rows_to_insert)
            total_rows += len(rows_to_insert)

    print(f"\n✓ Successfully loaded {total_rows} weather records")

    # Verify
    count_query = "SELECT count() FROM raw_weather_data"
    count = execute_clickhouse_query(count_query, verbose=False)
    print(f"✓ Total records in raw_weather_data table: {count.strip()}")


def insert_batch(rows):
    """Insert a batch of rows into ClickHouse"""
    data = '\n'.join(rows)
    url = f'http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}'
    params = {
        'database': CLICKHOUSE_DB,
        'query': '''INSERT INTO raw_weather_data (
            location_id, date, weather_code,
            temperature_2m_max, temperature_2m_min, temperature_2m_mean,
            apparent_temperature_max, apparent_temperature_min, apparent_temperature_mean,
            daylight_duration, sunshine_duration,
            precipitation_sum, rain_sum, precipitation_hours,
            wind_speed_10m_max, wind_gusts_10m_max, wind_direction_10m_dominant,
            shortwave_radiation_sum, et0_fao_evapotranspiration
        ) FORMAT TabSeparated'''
    }
    auth = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD) if CLICKHOUSE_USER else None

    response = requests.post(url, params=params, auth=auth, data=data.encode('utf-8'))
    response.raise_for_status()


def show_sample_data():
    """Display sample data from loaded tables"""
    print("\n" + "="*60)
    print("Sample Data")
    print("="*60)

    print("\n--- Locations (All Records) ---")
    query1 = """
    SELECT * FROM locations
    ORDER BY location_id
    FORMAT PrettyCompact
    """
    result = execute_clickhouse_query(query1, verbose=False)
    print(result)

    print("\n--- Weather Data (First 10 Records) ---")
    query2 = """
    SELECT
        location_id,
        date,
        weather_code,
        temperature_2m_mean,
        precipitation_hours,
        wind_speed_10m_max
    FROM raw_weather_data
    ORDER BY location_id, date
    LIMIT 10
    FORMAT PrettyCompact
    """
    result = execute_clickhouse_query(query2, verbose=False)
    print(result)

    print("\n--- Summary Statistics ---")
    query3 = """
    SELECT
        l.city_name,
        count() as record_count,
        round(avg(w.temperature_2m_mean), 2) as avg_temp,
        round(sum(w.precipitation_hours), 2) as total_precip_hours
    FROM raw_weather_data w
    JOIN locations l ON w.location_id = l.location_id
    GROUP BY l.city_name
    ORDER BY l.city_name
    FORMAT PrettyCompact
    """
    result = execute_clickhouse_query(query3, verbose=False)
    print(result)


def main():
    """Main function"""
    print("="*60)
    print("Load Raw Location and Weather Data to ClickHouse")
    print("="*60)

    # Create/replace tables
    drop_and_create_locations_table()
    drop_and_create_raw_weather_table()

    # Load data
    load_locations_data()
    load_weather_data()

    # Show samples
    show_sample_data()

    print("\n" + "="*60)
    print("Data loading completed!")
    print("="*60)
    print(f"\nClickHouse Web UI: http://localhost:8123/play")
    print(f"Database: {CLICKHOUSE_DB}")
    print(f"\nTables created:")
    print(f"  - weather_analytics.locations")
    print(f"  - weather_analytics.raw_weather_data")


if __name__ == "__main__":
    main()
