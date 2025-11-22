"""
Simple Hive Analysis Runner
Runs Hive queries directly via docker exec and loads results into ClickHouse
"""

import subprocess
import time
from datetime import datetime
from pathlib import Path
import requests
import csv

# ClickHouse Configuration
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DB = 'weather_analytics'
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = ''  # No password for ClickHouse

def execute_clickhouse_query(query):
    """Execute a query on ClickHouse"""
    url = f'http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}'
    params = {'database': CLICKHOUSE_DB}
    auth = None  # No authentication needed

    try:
        response = requests.post(url, params=params, auth=auth, data=query.encode('utf-8'))
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"    [X] ClickHouse Error: {e}")
        return None

def create_clickhouse_tables():
    """Create ClickHouse tables for Hive results if they don't exist"""
    print("\n[*] Creating ClickHouse tables...")

    # Table for Top 10 Temperate Cities
    table1_sql = """
    CREATE TABLE IF NOT EXISTS top_temperate_cities (
        city_name String,
        avg_max_temp Float64,
        analysis_timestamp DateTime,
        created_at DateTime DEFAULT now()
    ) ENGINE = MergeTree() ORDER BY (analysis_timestamp, avg_max_temp);
    """

    # Table for Evapotranspiration by Season
    table2_sql = """
    CREATE TABLE IF NOT EXISTS evapotranspiration_by_season (
        city_name String,
        season String,
        year UInt16,
        avg_evapotranspiration Float64,
        analysis_timestamp DateTime,
        created_at DateTime DEFAULT now()
    ) ENGINE = MergeTree() ORDER BY (city_name, year, season);
    """

    execute_clickhouse_query("""
    CREATE TABLE IF NOT EXISTS meta.updated_time (
        table_name String,
        last_updated_time DateTime
    ) ENGINE = MergeTree() ORDER BY table_name;
    """)
    print("    [OK] ClickHouse tables ready")

    result1 = execute_clickhouse_query(table1_sql)
    result2 = execute_clickhouse_query(table2_sql)

    if result1 is not None and result2 is not None:
        print("    [OK] ClickHouse tables ready")
        return True
    return False

def update_metadata_timestamp(table_name, timestamp):
    """Update ClickHouse metadata with latest timestamp for a table"""
    print(f"\n[*] Updating metadata for {table_name}...")

    # Delete old entry
    cmd_delete = f'docker exec clickhouse clickhouse-client --query="ALTER TABLE meta.updated_time DELETE WHERE table_name = \'{table_name}\'"'

    # Insert new entry
    cmd_insert = f'docker exec clickhouse clickhouse-client --query="INSERT INTO meta.updated_time (table_name, last_updated_time) VALUES (\'{table_name}\', \'{timestamp}\')"'

    try:
        subprocess.run(cmd_delete, shell=True, capture_output=True, text=True, check=True)
        subprocess.run(cmd_insert, shell=True, capture_output=True, text=True, check=True)
        print(f"    [OK] Metadata updated: {table_name} -> {timestamp}")
        return True
    except Exception as e:
        print(f"    [X] Error updating metadata: {e}")
        return False

def get_last_updated_time(table_name):
    """Get last updated time from ClickHouse metadata table"""
    cmd = f'docker exec clickhouse clickhouse-client --query="SELECT last_updated_time FROM meta.updated_time WHERE table_name = \'{table_name}\' LIMIT 1"'
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

    if result.returncode == 0 and result.stdout.strip():
        last_time = result.stdout.strip()
        return last_time
    else:
        return None

def get_new_hive_output_folders(last_updated_time=None):
    """Find all Hive output folders newer than last updated time"""
    print("\n[*] Finding new Hive analysis outputs from HDFS...")

    # Get all folders with timestamps for top_10_temperate_cities
    cmd = 'docker exec namenode bash -c "hdfs dfs -ls /user/data/hive_output/ | grep top_10_temperate_cities_"'
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

    if result.returncode != 0 or not result.stdout.strip():
        print("    [X] No folders found in HDFS")
        return []

    folders = []
    for line in result.stdout.strip().split('\n'):
        if 'top_10_temperate_cities_' in line:
            folder_path = line.split()[-1]
            folder_name = folder_path.split('/')[-1]
            # Extract timestamp from folder name: top_10_temperate_cities_20251115183603
            timestamp_str = folder_name.replace('top_10_temperate_cities_', '')

            # If we have a last updated time, filter by it
            if last_updated_time:
                # Convert datetime string to comparable format
                # From: "2025-11-15 18:36:03" to "20251115183603"
                last_time_formatted = last_updated_time.replace('-', '').replace(' ', '').replace(':', '')

                # Compare timestamps (format: YYYYMMDDHHmmss)
                print(f"    Comparing: {timestamp_str} vs {last_time_formatted}")
                if timestamp_str > last_time_formatted:
                    print(f"      -> Including {timestamp_str} (newer)")
                    folders.append(timestamp_str)
                else:
                    print(f"      -> Skipping {timestamp_str} (older or same)")
            else:
                print(f"      -> Including {timestamp_str} (no previous metadata)")
                folders.append(timestamp_str)

    if folders:
        print(f"    [OK] Found {len(folders)} new folder(s) to load")
    else:
        print("    [X] No new folders found")

    return folders

def load_to_clickhouse_top_cities(hdfs_output, timestamp_str):
    """Load top temperate cities data to ClickHouse from a specific HDFS folder"""
    try:
        # Convert timestamp string to datetime format
        # From: "20251115183603" to "2025-11-15 18:36:03"
        datetime_str = f"{timestamp_str[0:4]}-{timestamp_str[4:6]}-{timestamp_str[6:8]} {timestamp_str[8:10]}:{timestamp_str[10:12]}:{timestamp_str[12:14]}"

        # Get data from HDFS
        cmd = ['docker', 'exec', 'namenode', 'bash', '-c',
               f'hdfs dfs -cat {hdfs_output}/000000_0']
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)

        # Parse CSV data
        lines = result.stdout.strip().split('\n')
        print("------------------lines-", len(lines))
        records_inserted = 0

        for line in lines:
            if not line.strip():
                continue

            parts = line.split(',')
            if len(parts) >= 2:
                city_name = parts[0].strip()
                avg_temp = parts[1].strip()

                # Insert into ClickHouse
                insert_query = f"""
                INSERT INTO top_temperate_cities (city_name, avg_max_temp, analysis_timestamp)
                VALUES ('{city_name}', {avg_temp}, toDateTime('{datetime_str}'))
                """
                if execute_clickhouse_query(insert_query) is not None:
                    records_inserted += 1

        print(f"    [OK] Inserted {records_inserted} records from {timestamp_str}")
        return records_inserted

    except Exception as e:
        print(f"    [X] Error loading to ClickHouse: {e}")
        return 0

def load_to_clickhouse_evapotranspiration(hdfs_output, timestamp_str):
    """Load evapotranspiration data to ClickHouse from a specific HDFS folder"""
    try:
        # Convert timestamp string to datetime format
        datetime_str = f"{timestamp_str[0:4]}-{timestamp_str[4:6]}-{timestamp_str[6:8]} {timestamp_str[8:10]}:{timestamp_str[10:12]}:{timestamp_str[12:14]}"

        # Get data from HDFS
        cmd = ['docker', 'exec', 'namenode', 'bash', '-c',
               f'hdfs dfs -cat {hdfs_output}/000000_0']
        result = subprocess.run(cmd, capture_output=True, text=True)

        # Check if file exists
        if result.returncode != 0:
            print(f"    [!] File not found (skipping): {hdfs_output}")
            return 0

        # Parse CSV data
        lines = result.stdout.strip().split('\n')
        records_inserted = 0

        for line in lines:
            if not line.strip():
                continue

            parts = line.split(',')
            if len(parts) >= 4:
                city_name = parts[0].strip()
                season = parts[1].strip()
                year = parts[2].strip()
                avg_et = parts[3].strip()

                # Insert into ClickHouse
                insert_query = f"""
                INSERT INTO evapotranspiration_by_season (city_name, season, year, avg_evapotranspiration, analysis_timestamp)
                VALUES ('{city_name}', '{season}', {year}, {avg_et}, toDateTime('{datetime_str}'))
                """
                if execute_clickhouse_query(insert_query) is not None:
                    records_inserted += 1

        print(f"    [OK] Inserted {records_inserted} records from {timestamp_str}")
        return records_inserted

    except Exception as e:
        print(f"    [X] Error loading to ClickHouse: {e}")
        return 0

def run_hive_command(hive_sql, description):
    """Execute Hive SQL command"""
    print(f"\n[*] {description}...")
    cmd = ['docker', 'exec', 'hive-server', 'bash', '-c', f'hive -e "{hive_sql}"']
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=180)

    if result.returncode == 0:
        print(f"    [OK] Success")
        if result.stdout.strip():
            print(f"    Output: {result.stdout.strip()}")
        return True
    else:
        print(f"    [X] Error: {result.stderr[-500:]}")
        return False

def main():
    print("\n" + "="*60)
    print("Hive Weather Analytics Pipeline")
    print("="*60)

    # Step 0: Create ClickHouse tables
    create_clickhouse_tables()

    # Step 0.5: Check last updated time from metadata
    print("\n" + "="*60)
    print("Checking Metadata")
    print("="*60)
    last_updated_time = get_last_updated_time('top_temperate_cities')
    if last_updated_time:
        print(f"[*************************************] Last loaded data timestamp: {last_updated_time}")
    else:
        print(f"[*] No previous data found - will load all available folders")

    # Step 1: Create database
    run_hive_command(
        "CREATE DATABASE IF NOT EXISTS weather_analytics;",
        "Step 1: Creating database"
    )

    # Step 2: Create location table
    location_table_sql = """
USE weather_analytics;
CREATE EXTERNAL TABLE IF NOT EXISTS location_data (
    location_id INT,
    latitude DOUBLE,
    longitude DOUBLE,
    elevation INT,
    utc_offset_seconds INT,
    timezone STRING,
    timezone_abbreviation STRING,
    city_name STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/data/kafka_ingested/location'
TBLPROPERTIES ('skip.header.line.count'='1');
"""
    run_hive_command(location_table_sql, "Step 2: Creating location table")

    # Step 3: Create weather table ('date' is a reserved keyword, use `date`)
    weather_table_sql = """
USE weather_analytics;
DROP TABLE IF EXISTS weather_data;

CREATE EXTERNAL TABLE weather_data (
    location_id INT,
    dt STRING,
    weather_code INT,
    temperature_2m_max DOUBLE,
    temperature_2m_min DOUBLE,
    temperature_2m_mean DOUBLE,
    apparent_temperature_max DOUBLE,
    apparent_temperature_min DOUBLE,
    apparent_temperature_mean DOUBLE,
    daylight_duration DOUBLE,
    sunshine_duration DOUBLE,
    precipitation_sum DOUBLE,
    rain_sum DOUBLE,
    precipitation_hours DOUBLE,
    wind_speed_10m_max DOUBLE,
    wind_gusts_10m_max DOUBLE,
    wind_direction_10m_dominant DOUBLE,
    shortwave_radiation_sum DOUBLE,
    et0_fao_evapotranspiration DOUBLE,
    sunrise STRING,
    sunset STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/data/kafka_ingested/weather'
TBLPROPERTIES ('skip.header.line.count'='1');
"""
    run_hive_command(weather_table_sql, "Step 3: Creating weather table")

    # Step 4: Verify tables
    print("\n[*] Step 4: Verifying tables...")
    subprocess.run(['docker', 'exec', 'hive-server', 'bash', '-c',
                   'hive -e "USE weather_analytics; SHOW TABLES;"'])

    # Step 5: Run Query 1 - Top 10 Most Temperate Cities
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    print(f"\n[*] Timestamp: {timestamp}")

    print("\n[*] Step 5: Running Query 1 - Top 10 Most Temperate Cities...")

    hdfs_output1 = f"/user/data/hive_output/top_10_temperate_cities_{timestamp}"
    query1_sql = f"""
    USE weather_analytics;
    INSERT OVERWRITE DIRECTORY '{hdfs_output1}'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    SELECT l.city_name, ROUND(AVG(w.temperature_2m_max), 2) as avg_temp
    FROM weather_data w JOIN location_data l ON w.location_id = l.location_id
    WHERE w.temperature_2m_max IS NOT NULL
    GROUP BY l.city_name
    ORDER BY avg_temp ASC
    LIMIT 10;
    """
    run_hive_command(query1_sql, "Executing Query 1")

    # Step 6: Run Query 2 - Evapotranspiration by Season
    print("\n[*] Step 6: Running Query 2 - Avg Evapotranspiration by Season...")

    hdfs_output2 = f"/user/data/hive_output/avg_evapotranspiration_by_season_{timestamp}"
    # Simpler query without complex subquery for Hive 2.3 compatibility
    query2_sql = f"""
USE weather_analytics;

INSERT OVERWRITE DIRECTORY '{hdfs_output2}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','

SELECT
    l.city_name,
    CASE
        WHEN CAST(split(w.dt, '/')[0] AS INT) IN (1,2,3,9,10,11,12) THEN 'Maha'
        ELSE 'Yala'
    END AS season,
    CAST(split(w.dt, '/')[2] AS INT) AS year,
    ROUND(AVG(w.et0_fao_evapotranspiration), 2) AS avg_evapotranspiration
FROM weather_data w
JOIN location_data l ON w.location_id = l.location_id
WHERE w.et0_fao_evapotranspiration IS NOT NULL
  AND w.dt IS NOT NULL
  AND w.dt RLIKE '^[0-9]+/[0-9]+/[0-9]+$'
GROUP BY
    l.city_name,
    split(w.dt, '/')[2],
    CASE
        WHEN CAST(split(w.dt, '/')[0] AS INT) IN (1,2,3,9,10,11,12) THEN 'Maha'
        ELSE 'Yala'
    END
ORDER BY l.city_name, year, season;
"""

    run_hive_command(query2_sql, "Executing Query 2")

    # Step 7: Find new folders from HDFS and load to ClickHouse
    print("\n" + "="*60)
    print("Step 7: Loading New Results to ClickHouse")
    print("="*60)

    # Get list of new folders from HDFS (including the one just created)
    new_folders = get_new_hive_output_folders(last_updated_time)

    print("++++++++++++++++++++++",new_folders)

    if new_folders:
        total_records_q1 = 0
        total_records_q2 = 0

        for folder_timestamp in new_folders:
            print(f"\n[*] Processing folder with timestamp: {folder_timestamp}")

            # Load Query 1 results
            hdfs_path1 = f"/user/data/hive_output/top_10_temperate_cities_{folder_timestamp}"
            print(f"  Loading from: {hdfs_path1}")
            records_q1 = load_to_clickhouse_top_cities(hdfs_path1, folder_timestamp)
            total_records_q1 += records_q1

            # Load Query 2 results
            hdfs_path2 = f"/user/data/hive_output/avg_evapotranspiration_by_season_{folder_timestamp}"
            print(f"  Loading from: {hdfs_path2}")
            records_q2 = load_to_clickhouse_evapotranspiration(hdfs_path2, folder_timestamp)
            total_records_q2 += records_q2

        print(f"\n[*] Total records loaded:")
        print(f"    Query 1 (Top Cities): {total_records_q1}")
        print(f"    Query 2 (Evapotranspiration): {total_records_q2}")

        # Update metadata with the latest timestamp
        if total_records_q1 > 0 or total_records_q2 > 0:
            latest_timestamp = max(new_folders)
            # Convert to datetime format
            datetime_str = f"{latest_timestamp[0:4]}-{latest_timestamp[4:6]}-{latest_timestamp[6:8]} {latest_timestamp[8:10]}:{latest_timestamp[10:12]}:{latest_timestamp[12:14]}"

            print(f"\n[*] Updating metadata with latest timestamp: {datetime_str}")
            update_metadata_timestamp('top_temperate_cities', datetime_str)
            update_metadata_timestamp('evapotranspiration_by_season', datetime_str)
    else:
        print("\n[*] No new folders to load - all data is already up to date")

    # Step 8: Copy new results from HDFS to local
    print("\n" + "="*60)
    print("Step 8: Copying New Results to Local Folder")
    print("="*60)
    Path("./hive_results").mkdir(exist_ok=True)

    if new_folders:
        for folder_timestamp in new_folders:
            # Query 1 results
            local_file1 = f"hive_results/top_10_temperate_cities_{folder_timestamp}.csv"
            subprocess.run(['docker', 'exec', 'namenode', 'bash', '-c',
                           f'hdfs dfs -get -f /user/data/hive_output/top_10_temperate_cities_{folder_timestamp}/000000_0 /tmp/query1.csv'])
            subprocess.run(['docker', 'cp', 'namenode:/tmp/query1.csv', local_file1])
            print(f"    [OK] Saved: {local_file1}")

            # Query 2 results
            local_file2 = f"hive_results/avg_evapotranspiration_by_season_{folder_timestamp}.csv"
            subprocess.run(['docker', 'exec', 'namenode', 'bash', '-c',
                           f'hdfs dfs -get -f /user/data/hive_output/avg_evapotranspiration_by_season_{folder_timestamp}/000000_0 /tmp/query2.csv'])
            subprocess.run(['docker', 'cp', 'namenode:/tmp/query2.csv', local_file2])
            print(f"    [OK] Saved: {local_file2}")
    else:
        print("    [*] No new files to copy")

    # Step 9: Display results
    print("\n" + "="*60)
    print("Analysis Complete!")
    print("="*60)

    if new_folders:
        latest_timestamp = max(new_folders)
        print(f"\nLatest analysis timestamp: {latest_timestamp}")
        print(f"\nNew results saved to hive_results/ folder:")
        for folder_timestamp in new_folders:
            print(f"  - top_10_temperate_cities_{folder_timestamp}.csv")
            print(f"  - avg_evapotranspiration_by_season_{folder_timestamp}.csv")

        print(f"\nHDFS locations (latest):")
        print(f"  1. /user/data/hive_output/top_10_temperate_cities_{latest_timestamp}")
        print(f"  2. /user/data/hive_output/avg_evapotranspiration_by_season_{latest_timestamp}")

        # Show sample results from the latest folder
        print("\n[*] Latest Query 1 Results (Top 10 Most Temperate Cities):")
        print("-"*60)
        subprocess.run(['docker', 'exec', 'namenode', 'bash', '-c',
                       f'hdfs dfs -cat /user/data/hive_output/top_10_temperate_cities_{latest_timestamp}/000000_0'])

        print("\n[*] Latest Query 2 Results (First 20 rows):")
        print("-"*60)
        subprocess.run(['docker', 'exec', 'namenode', 'bash', '-c',
                       f'hdfs dfs -cat /user/data/hive_output/avg_evapotranspiration_by_season_{latest_timestamp}/000000_0 | head -20'])
    else:
        print("\n[*] No new analysis was performed. All data is already loaded.")

    # Display ClickHouse summary
    print("\n" + "="*60)
    print("ClickHouse Data Warehouse Summary")
    print("="*60)

    # Get record counts from ClickHouse
    count_query1 = "SELECT count() FROM top_temperate_cities"
    count_query2 = "SELECT count() FROM evapotranspiration_by_season"

    count1 = execute_clickhouse_query(count_query1)
    count2 = execute_clickhouse_query(count_query2)

    if count1 and count2:
        print(f"\n[*] Total records in ClickHouse:")
        print(f"    top_temperate_cities: {count1.strip()}")
        print(f"    evapotranspiration_by_season: {count2.strip()}")

    # Display last updated times from metadata
    print("\n" + "-"*60)
    print("Last Updated Times (from metadata)")
    print("-"*60)
    final_last_update_1 = get_last_updated_time('top_temperate_cities')
    final_last_update_2 = get_last_updated_time('evapotranspiration_by_season')

    if final_last_update_1:
        print(f"[*] top_temperate_cities: {final_last_update_1}")
    if final_last_update_2:
        print(f"[*] evapotranspiration_by_season: {final_last_update_2}")

    print(f"\n[*] ClickHouse Web UI: http://localhost:8123/play")
    print(f"[*] Database: {CLICKHOUSE_DB}")

    print("\n" + "="*60)
    print("Pipeline Complete!")
    print("="*60)

if __name__ == "__main__":
    main()


# """
# Simple Hive Analysis Runner - FINAL WORKING VERSION
# Tested on Hive 2.1.1 + ClickHouse + Docker (2025)
# """
# import subprocess
# import time
# from datetime import datetime
# from pathlib import Path
# import requests

# # ClickHouse Configuration
# CLICKHOUSE_HOST = 'localhost'
# CLICKHOUSE_PORT = 8123
# CLICKHOUSE_DB = 'weather_analytics'
# CLICKHOUSE_USER = 'default'
# CLICKHOUSE_PASSWORD = ''  # Empty password (default ClickHouse Docker)

# def execute_clickhouse_query(query):
#     url = f'http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}'
#     params = {'database': CLICKHOUSE_DB}
#     try:
#         response = requests.post(
#             url,
#             params=params,
#             auth=('default', ''),  # Explicitly send empty password
#             data=query.encode('utf-8'),
#             timeout=10
#         )
#         response.raise_for_status()
#         return response.text
#     except Exception as e:
#         print(f"    [X] ClickHouse Error: {e}")
#         return None

# def create_clickhouse_tables():
#     print("\n[*] Creating ClickHouse tables and metadata...")
#     # Create main tables
#     execute_clickhouse_query("""
#     CREATE TABLE IF NOT EXISTS top_temperate_cities (
#         city_name String,
#         avg_max_temp Float64,
#         analysis_timestamp DateTime,
#         created_at DateTime DEFAULT now()
#     ) ENGINE = MergeTree() ORDER BY (analysis_timestamp, avg_max_temp);
#     """)
#     execute_clickhouse_query("""
#     CREATE TABLE IF NOT EXISTS evapotranspiration_by_season (
#         city_name String,
#         season String,
#         year UInt16,
#         avg_evapotranspiration Float64,
#         analysis_timestamp DateTime,
#         created_at DateTime DEFAULT now()
#     ) ENGINE = MergeTree() ORDER BY (city_name, year, season);
#     """)
#     # Create metadata table
#     execute_clickhouse_query("""
#     CREATE TABLE IF NOT EXISTS meta.updated_time (
#         table_name String,
#         last_updated_time DateTime
#     ) ENGINE = MergeTree() ORDER BY table_name;
#     """)
#     print("    [OK] ClickHouse tables ready")



# def get_last_updated_time(table_name):
#     """Get last updated time from ClickHouse metadata table"""
#     cmd = f'docker exec clickhouse clickhouse-client --query="SELECT last_updated_time FROM meta.updated_time WHERE table_name = \'{table_name}\' LIMIT 1"'
#     result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

#     if result.returncode == 0 and result.stdout.strip():
#         last_time = result.stdout.strip()
#         return last_time
#     else:
#         return None

# def update_metadata_timestamp(table_name, timestamp):
#     """Update ClickHouse metadata with latest timestamp for a table"""
#     print(f"\n[*] Updating metadata for {table_name}...")

#     # Delete old entry
#     cmd_delete = f'docker exec clickhouse clickhouse-client --query="ALTER TABLE meta.updated_time DELETE WHERE table_name = \'{table_name}\'"'

#     # Insert new entry
#     cmd_insert = f'docker exec clickhouse clickhouse-client --query="INSERT INTO meta.updated_time (table_name, last_updated_time) VALUES (\'{table_name}\', \'{timestamp}\')"'

#     try:
#         subprocess.run(cmd_delete, shell=True, capture_output=True, text=True, check=True)
#         subprocess.run(cmd_insert, shell=True, capture_output=True, text=True, check=True)
#         print(f"    [OK] Metadata updated: {table_name} -> {timestamp}")
#         return True
#     except Exception as e:
#         print(f"    [X] Error updating metadata: {e}")
#         return False
    

    

# def run_hive_command(hive_sql, description):
#     print(f"\n[*] {description}...")
#     cmd = ['docker', 'exec', 'hive-server', 'bash', '-c', f'hive -e "{hive_sql}"']
#     result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
#     if result.returncode == 0:
#         print("    [OK] Success")
#         return True
#     else:
#         print(f"    [X] Error: {result.stderr[-600:]}")
#         return False

# def main():
#     print("\n" + "="*70)
#     print("    HIVE WEATHER ANALYTICS PIPELINE - FULLY WORKING VERSION")
#     print("="*70)

#     create_clickhouse_tables()

#     print("\n[*] Step 1-3: Creating database and tables (with forced cleanup)...")

#     # Step 1: Database
#     run_hive_command("CREATE DATABASE IF NOT EXISTS weather_analytics;", "Creating database")

#     # Step 2: Location table (safe)
#     run_hive_command("""
#     USE weather_analytics;
#     DROP TABLE IF EXISTS location_data;
#     CREATE EXTERNAL TABLE location_data (
#         location_id INT,
#         latitude DOUBLE,
#         longitude DOUBLE,
#         elevation INT,
#         utc_offset_seconds INT,
#         timezone STRING,
#         timezone_abbreviation STRING,
#         city_name STRING
#     )
#     ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
#     STORED AS TEXTFILE
#     LOCATION '/user/data/kafka_ingested/location'
#     TBLPROPERTIES ('skip.header.line.count'='1');
#     """, "Creating location table")

#     # Step 3: Weather table - THE FIX THAT SOLVES YOUR MAIN ERROR
#     # Using 'dt' instead of `date` to avoid bash backtick interpretation
#     weather_table_sql = """
# USE weather_analytics;
# DROP TABLE IF EXISTS weather_data;

# CREATE EXTERNAL TABLE weather_data (
#     location_id INT,
#     dt STRING,
#     weather_code INT,
#     temperature_2m_max DOUBLE,
#     temperature_2m_min DOUBLE,
#     temperature_2m_mean DOUBLE,
#     apparent_temperature_max DOUBLE,
#     apparent_temperature_min DOUBLE,
#     apparent_temperature_mean DOUBLE,
#     daylight_duration DOUBLE,
#     sunshine_duration DOUBLE,
#     precipitation_sum DOUBLE,
#     rain_sum DOUBLE,
#     precipitation_hours DOUBLE,
#     wind_speed_10m_max DOUBLE,
#     wind_gusts_10m_max DOUBLE,
#     wind_direction_10m_dominant DOUBLE,
#     shortwave_radiation_sum DOUBLE,
#     et0_fao_evapotranspiration DOUBLE,
#     sunrise STRING,
#     sunset STRING
# )
# ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
# STORED AS TEXTFILE
# LOCATION '/user/data/kafka_ingested/weather'
# TBLPROPERTIES ('skip.header.line.count'='1');
# """
#     run_hive_command(weather_table_sql, "Step 3: Creating weather table")
#     # Verify
#     print("\n[*] Verifying tables...")
#     subprocess.run(['docker', 'exec', 'hive-server', 'bash', '-c',
#                    'hive -e "USE weather_analytics; SHOW TABLES;"'], timeout=60)

#     # Generate timestamp
#     timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
#     print(f"\n[*] Analysis Timestamp: {timestamp}")

#     # Query 1 - Top 10 Temperate Cities
#     print("\n[*] Running Query 1 - Top 10 Most Temperate Cities...")
#     output1 = f"/user/data/hive_output/top_10_temperate_cities_{timestamp}"
#     run_hive_command(f"""
#     USE weather_analytics;
#     INSERT OVERWRITE DIRECTORY '{output1}'
#     ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
#     SELECT l.city_name, ROUND(AVG(w.temperature_2m_max), 2) as avg_temp
#     FROM weather_data w JOIN location_data l ON w.location_id = l.location_id
#     WHERE w.temperature_2m_max IS NOT NULL
#     GROUP BY l.city_name
#     ORDER BY avg_temp ASC
#     LIMIT 10;
#     """, "Executing Query 1")

#     # Query 2 - Evapotranspiration by Season (WORKS ON HIVE 2.1.1)
#     print("\n[*] Running Query 2 - Avg Evapotranspiration by Season...")
#     output2 = f"/user/data/hive_output/avg_evapotranspiration_by_season_{timestamp}"

#     query2_sql = f"""
# USE weather_analytics;

# INSERT OVERWRITE DIRECTORY '{output2}'
# ROW FORMAT DELIMITED FIELDS TERMINATED BY ','

# SELECT
#     l.city_name,
#     CASE
#         WHEN CAST(split(w.dt, '/')[0] AS INT) IN (1,2,3,9,10,11,12) THEN 'Maha'
#         ELSE 'Yala'
#     END AS season,
#     CAST(split(w.dt, '/')[2] AS INT) AS year,
#     ROUND(AVG(w.et0_fao_evapotranspiration), 2) AS avg_evapotranspiration
# FROM weather_data w
# JOIN location_data l ON w.location_id = l.location_id
# WHERE w.et0_fao_evapotranspiration IS NOT NULL
#   AND w.dt IS NOT NULL
#   AND w.dt RLIKE '^[0-9]+/[0-9]+/[0-9]+$'
# GROUP BY
#     l.city_name,
#     split(w.dt, '/')[2],
#     CASE
#         WHEN CAST(split(w.dt, '/')[0] AS INT) IN (1,2,3,9,10,11,12) THEN 'Maha'
#         ELSE 'Yala'
#     END
# ORDER BY l.city_name, year, season;
# """

#     run_hive_command(query2_sql, "Executing Query 2 - Avg Evapotranspiration by Season")

#     # Load to ClickHouse
#     print("\n" + "="*60)
#     print("Loading results into ClickHouse...")
#     print("="*60)

#     def load_file(path, timestamp_str, is_query1=True):
#         dt = f"{timestamp_str[0:4]}-{timestamp_str[4:6]}-{timestamp_str[6:8]} {timestamp_str[8:10]}:{timestamp_str[10:12]}:{timestamp_str[12:14]}"
#         cmd = f'docker exec namenode hdfs dfs -cat {path}/000000_0'
#         result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
#         if result.returncode != 0:
#             print(f"    [X] No file: {path}")
#             return 0
#         lines = [l for l in result.stdout.strip().split('\n') if l.strip()]
#         inserted = 0
#         for line in lines:
#             parts = [p.strip() for p in line.split(',')]
#             if is_query1 and len(parts) >= 2:
#                 q = f"INSERT INTO top_temperate_cities (city_name, avg_max_temp, analysis_timestamp) VALUES ('{parts[0]}', {parts[1]}, toDateTime('{dt}'))"
#             elif not is_query1 and len(parts) >= 4:
#                 q = f"INSERT INTO evapotranspiration_by_season (city_name, season, year, avg_evapotranspiration, analysis_timestamp) VALUES ('{parts[0]}', '{parts[1]}', {parts[2]}, {parts[3]}, toDateTime('{dt}'))"
#             else:
#                 continue
#             if execute_clickhouse_query(q) is not None:
#                 inserted += 1
#         print(f"    [OK] Inserted {inserted} records -> {path.split('_')[-1]}")
#         return inserted

#     q1_count = load_file(output1, timestamp, True)
#     q2_count = load_file(output2, timestamp, False)

#     # Save locally
#     Path("hive_results").mkdir(exist_ok=True)
#     for name, path in [(f"top_10_temperate_cities_{timestamp}.csv", output1),
#                        (f"avg_evapotranspiration_by_season_{timestamp}.csv", output2)]:
#         subprocess.run(['docker', 'exec', 'namenode', 'bash', '-c',
#                        f'hdfs dfs -get {path}/000000_0 /tmp/out.csv || true'], timeout=30)
#         subprocess.run(['docker', 'cp', 'namenode:/tmp/out.csv', f"hive_results/{name}"], timeout=30)
#         print(f"    [OK] Saved: hive_results/{name}")

#     # Final summary
#     print("\n" + "="*70)
#     print("PIPELINE COMPLETED SUCCESSFULLY!")
#     print("="*70)
#     print(f"    Latest results saved with timestamp: {timestamp}")
#     print(f"    Query 1 records in ClickHouse: {q1_count}")
#     print(f"    Query 2 records in ClickHouse: {q2_count}")
#     print(f"    Check results in: hive_results/")
#     print(f"    ClickHouse UI: http://localhost:8123/play")
#     print("="*70)

# if __name__ == "__main__":
#     main()
