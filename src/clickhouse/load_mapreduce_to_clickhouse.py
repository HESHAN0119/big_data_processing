#!/usr/bin/env python3
"""
Load MapReduce results from HDFS into ClickHouse Data Warehouse
"""

import subprocess
import requests
import tempfile
import os
from datetime import datetime

# Configuration
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DB = 'weather_analytics'
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = ''

def execute_clickhouse_query(query):
    """Execute a query on ClickHouse"""
    url = f'http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}'
    params = {'database': CLICKHOUSE_DB}
    auth = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD) if CLICKHOUSE_USER else None

    try:
        response = requests.post(url, params=params, auth=auth, data=query.encode('utf-8'))
        response.raise_for_status()
        print(f"✓ Query executed successfully")
        return response.text
    except Exception as e:
        print(f"✗ Error executing query: {e}")
        return None

def load_district_monthly_weather():
    """Load District Monthly Weather data from HDFS to ClickHouse"""
    print("\n" + "="*60)
    print("Loading District Monthly Weather Data")
    print("="*60)

    # Copy data from HDFS to local temp file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.tsv') as tmp_file:
        tmp_path = tmp_file.name

    try:
        # Get data from HDFS
        cmd = ['docker', 'exec', 'namenode', 'bash', '-c',
               'hdfs dfs -cat /user/data/mapreduce_output/district_monthly/part-*']

        result = subprocess.run(cmd, capture_output=True, text=True, check=True)

        # Parse and format data
        lines = result.stdout.strip().split('\n')
        formatted_data = []

        for line in lines:
            if not line.strip():
                continue

            # Format: District\tYear-Month\tPrecip\tTemp
            parts = line.split('\t')
            if len(parts) >= 4:
                district = parts[0].strip()
                year_month = parts[1].strip()
                precip = parts[2].strip()
                temp = parts[3].strip()

                # Split year-month
                year, month = year_month.split('-')

                formatted_data.append(f"{district}\t{year}\t{month}\t{year_month}\t{precip}\t{temp}")

        # Write formatted data
        with open(tmp_path, 'w') as f:
            f.write('\n'.join(formatted_data))

        print(f"Parsed {len(formatted_data)} records")

        # Load into ClickHouse
        with open(tmp_path, 'rb') as f:
            url = f'http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}'
            params = {
                'database': CLICKHOUSE_DB,
                'query': 'INSERT INTO district_monthly_weather (district, year, month, year_month, total_precipitation_hours, mean_temperature) FORMAT TabSeparated'
            }
            auth = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD) if CLICKHOUSE_USER else None
            response = requests.post(url, params=params, auth=auth, data=f)
            response.raise_for_status()

        print(f"✓ Successfully loaded {len(formatted_data)} records into ClickHouse")

        # Verify
        count_query = "SELECT count() FROM district_monthly_weather"
        count = execute_clickhouse_query(count_query)
        print(f"✓ Total records in table: {count.strip()}")

    except subprocess.CalledProcessError as e:
        print(f"✗ Error reading from HDFS: {e}")
        print(f"Output: {e.stdout}")
        print(f"Error: {e.stderr}")
    except Exception as e:
        print(f"✗ Error loading data: {e}")
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)

def load_highest_precipitation():
    """Load Highest Precipitation data from HDFS to ClickHouse"""
    print("\n" + "="*60)
    print("Loading Highest Precipitation Data")
    print("="*60)

    try:
        # Get data from HDFS
        cmd = ['docker', 'exec', 'namenode', 'bash', '-c',
               'hdfs dfs -cat /user/data/mapreduce_output/highest_precipitation/part-*']

        result = subprocess.run(cmd, capture_output=True, text=True, check=True)

        # Parse data
        lines = result.stdout.strip().split('\n')

        for line in lines:
            if not line.strip():
                continue

            # Format: Year-Month\tPrecip
            parts = line.split('\t')
            if len(parts) >= 2:
                year_month = parts[0].strip()
                precip = parts[1].strip()

                # Split year-month
                year, month = year_month.split('-')

                # Insert into ClickHouse
                insert_query = f"""
                INSERT INTO highest_precipitation (year, month, year_month, total_precipitation_hours)
                VALUES ({year}, {month}, '{year_month}', {precip})
                """
                execute_clickhouse_query(insert_query)

        print(f"✓ Successfully loaded highest precipitation record")

        # Show the result
        result_query = "SELECT * FROM highest_precipitation ORDER BY total_precipitation_hours DESC LIMIT 1"
        result = execute_clickhouse_query(result_query)
        print(f"\nHighest Precipitation Record:")
        print(result)

    except subprocess.CalledProcessError as e:
        print(f"✗ Error reading from HDFS: {e}")
        print(f"Output: {e.stdout}")
        print(f"Error: {e.stderr}")
    except Exception as e:
        print(f"✗ Error loading data: {e}")

def show_sample_data():
    """Show sample data from ClickHouse tables"""
    print("\n" + "="*60)
    print("Sample Data from ClickHouse")
    print("="*60)

    print("\n--- District Monthly Weather (First 10 records) ---")
    query1 = """
    SELECT district, year_month, total_precipitation_hours, mean_temperature
    FROM district_monthly_weather
    ORDER BY district, year, month
    LIMIT 10
    FORMAT PrettyCompact
    """
    result = execute_clickhouse_query(query1)
    print(result)

    print("\n--- Top 5 Districts by Average Temperature ---")
    query2 = """
    SELECT
        district,
        round(avg(mean_temperature), 2) as avg_temp,
        round(sum(total_precipitation_hours), 2) as total_precip
    FROM district_monthly_weather
    GROUP BY district
    ORDER BY avg_temp DESC
    LIMIT 5
    FORMAT PrettyCompact
    """
    result = execute_clickhouse_query(query2)
    print(result)

    print("\n--- Top 5 Months with Highest Precipitation ---")
    query3 = """
    SELECT
        district,
        year_month,
        round(total_precipitation_hours, 2) as precip_hours
    FROM district_monthly_weather
    ORDER BY total_precipitation_hours DESC
    LIMIT 5
    FORMAT PrettyCompact
    """
    result = execute_clickhouse_query(query3)
    print(result)

def main():
    """Main function"""
    print("="*60)
    print("MapReduce to ClickHouse Data Warehouse Loader")
    print("="*60)

    # Load data
    load_district_monthly_weather()
    load_highest_precipitation()

    # Show sample queries
    show_sample_data()

    print("\n" + "="*60)
    print("Data loading completed!")
    print("="*60)
    print(f"\nClickHouse Web UI: http://localhost:8123/play")
    print(f"Database: {CLICKHOUSE_DB}")

if __name__ == "__main__":
    main()
