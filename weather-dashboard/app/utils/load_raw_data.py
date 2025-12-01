"""
Script to load raw weather data from CSV files into ClickHouse
This populates the raw_weather_data table needed for Requirement 4 (extreme weather events)
"""

import pandas as pd
from clickhouse_driver import Client
import os
from datetime import datetime

# ClickHouse connection settings
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', 9000))
CLICKHOUSE_DB = os.getenv('CLICKHOUSE_DATABASE', 'weather_analytics')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')

def connect_clickhouse():
    """Connect to ClickHouse"""
    try:
        client = Client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            database=CLICKHOUSE_DB,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD
        )
        print(f"✅ Connected to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")
        return client
    except Exception as e:
        print(f"❌ Failed to connect to ClickHouse: {e}")
        return None

def load_weather_data(client, csv_path):
    """Load weather data from CSV into ClickHouse"""

    print(f"\n📂 Loading weather data from: {csv_path}")

    if not os.path.exists(csv_path):
        print(f"❌ File not found: {csv_path}")
        return False

    try:
        # Read CSV
        df = pd.read_csv(csv_path)
        print(f"📊 Loaded {len(df)} records from CSV")

        # Convert date format from M/D/YYYY to YYYY-MM-DD
        df['date_parsed'] = pd.to_datetime(df['date'], format='%m/%d/%Y').dt.date

        # Prepare data for insertion
        records = []
        for _, row in df.iterrows():
            record = (
                int(row['location_id']) if pd.notna(row['location_id']) else 0,
                row['date_parsed'],
                int(row['weather_code (wmo code)']) if pd.notna(row['weather_code (wmo code)']) else 0,
                float(row['temperature_2m_max (°C)']) if pd.notna(row['temperature_2m_max (°C)']) else 0.0,
                float(row['temperature_2m_min (°C)']) if pd.notna(row['temperature_2m_min (°C)']) else 0.0,
                float(row['temperature_2m_mean (°C)']) if pd.notna(row['temperature_2m_mean (°C)']) else 0.0,
                float(row['apparent_temperature_max (°C)']) if pd.notna(row['apparent_temperature_max (°C)']) else 0.0,
                float(row['apparent_temperature_min (°C)']) if pd.notna(row['apparent_temperature_min (°C)']) else 0.0,
                float(row['apparent_temperature_mean (°C)']) if pd.notna(row['apparent_temperature_mean (°C)']) else 0.0,
                float(row['daylight_duration (s)']) if pd.notna(row['daylight_duration (s)']) else 0.0,
                float(row['sunshine_duration (s)']) if pd.notna(row['sunshine_duration (s)']) else 0.0,
                float(row['precipitation_sum (mm)']) if pd.notna(row['precipitation_sum (mm)']) else 0.0,
                float(row['rain_sum (mm)']) if pd.notna(row['rain_sum (mm)']) else 0.0,
                float(row['precipitation_hours (h)']) if pd.notna(row['precipitation_hours (h)']) else 0.0,
                float(row['wind_speed_10m_max (km/h)']) if pd.notna(row['wind_speed_10m_max (km/h)']) else 0.0,
                float(row['wind_gusts_10m_max (km/h)']) if pd.notna(row['wind_gusts_10m_max (km/h)']) else 0.0,
                float(row['wind_direction_10m_dominant (°)']) if pd.notna(row['wind_direction_10m_dominant (°)']) else 0.0,
                float(row['shortwave_radiation_sum (MJ/m²)']) if pd.notna(row['shortwave_radiation_sum (MJ/m²)']) else 0.0,
                float(row['et0_fao_evapotranspiration (mm)']) if pd.notna(row['et0_fao_evapotranspiration (mm)']) else 0.0
            )
            records.append(record)

        # Insert into ClickHouse
        insert_query = """
        INSERT INTO raw_weather_data (
            location_id, date, weather_code,
            temperature_2m_max, temperature_2m_min, temperature_2m_mean,
            apparent_temperature_max, apparent_temperature_min, apparent_temperature_mean,
            daylight_duration, sunshine_duration,
            precipitation_sum, rain_sum, precipitation_hours,
            wind_speed_10m_max, wind_gusts_10m_max, wind_direction_10m_dominant,
            shortwave_radiation_sum, et0_fao_evapotranspiration
        ) VALUES
        """

        print(f"⏳ Inserting {len(records)} records into ClickHouse...")
        client.execute(insert_query, records)

        print(f"✅ Successfully inserted {len(records)} records")
        return True

    except Exception as e:
        print(f"❌ Error loading weather data: {e}")
        import traceback
        traceback.print_exc()
        return False

def load_location_data(client, csv_path):
    """Load location data from CSV into ClickHouse"""

    print(f"\n📂 Loading location data from: {csv_path}")

    if not os.path.exists(csv_path):
        print(f"❌ File not found: {csv_path}")
        return False

    try:
        # Read CSV
        df = pd.read_csv(csv_path)
        print(f"📊 Loaded {len(df)} locations from CSV")

        # Prepare data for insertion
        records = []
        for _, row in df.iterrows():
            record = (
                int(row['location_id']),
                float(row['latitude']),
                float(row['longitude']),
                float(row['elevation']),
                int(row['utc_offset_seconds']),
                str(row['timezone']),
                str(row['timezone_abbreviation']),
                str(row['city_name'])
            )
            records.append(record)

        # Insert into ClickHouse
        insert_query = """
        INSERT INTO locations (
            location_id, latitude, longitude, elevation,
            utc_offset_seconds, timezone, timezone_abbreviation, city_name
        ) VALUES
        """

        print(f"⏳ Inserting {len(records)} locations into ClickHouse...")
        client.execute(insert_query, records)

        print(f"✅ Successfully inserted {len(records)} locations")
        return True

    except Exception as e:
        print(f"❌ Error loading location data: {e}")
        import traceback
        traceback.print_exc()
        return False

def verify_data(client):
    """Verify loaded data"""
    print("\n🔍 Verifying loaded data...")

    try:
        # Check raw_weather_data count
        result = client.execute("SELECT count() FROM raw_weather_data")
        weather_count = result[0][0]
        print(f"  raw_weather_data: {weather_count} records")

        # Check locations count
        result = client.execute("SELECT count() FROM locations")
        location_count = result[0][0]
        print(f"  locations: {location_count} records")

        # Check extreme weather events
        result = client.execute("""
            SELECT count() FROM raw_weather_data
            WHERE precipitation_sum > 30 AND wind_gusts_10m_max > 50
        """)
        extreme_count = result[0][0]
        print(f"  extreme weather events: {extreme_count} records")

        return True
    except Exception as e:
        print(f"❌ Error verifying data: {e}")
        return False

def main():
    """Main function"""
    print("=" * 60)
    print("Weather Data Loader for ClickHouse")
    print("=" * 60)

    # Connect to ClickHouse
    client = connect_clickhouse()
    if not client:
        return

    # Define data paths (adjust as needed)
    # These paths assume the script is run from the dashboard container
    # and the main project data folder is mounted
    weather_csv = "/data/weatherData.csv"
    location_csv = "/data/locationData_3.csv"

    # Alternative paths if running locally
    if not os.path.exists(weather_csv):
        weather_csv = "../data/weatherData.csv"
    if not os.path.exists(location_csv):
        location_csv = "../data/locationData_3.csv"

    # Load location data first (needed for joins)
    success = load_location_data(client, location_csv)
    if not success:
        print("\n⚠️  Failed to load location data. Continuing anyway...")

    # Load weather data
    success = load_weather_data(client, weather_csv)
    if not success:
        print("\n❌ Failed to load weather data.")
        return

    # Verify data
    verify_data(client)

    print("\n" + "=" * 60)
    print("✅ Data loading complete!")
    print("=" * 60)

if __name__ == "__main__":
    main()
