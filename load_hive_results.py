"""
Load Hive Analysis Results from HDFS to ClickHouse
Automatically loads only new results based on metadata timestamps
"""

import subprocess
import os
from pathlib import Path
import requests

# ClickHouse Configuration
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DB = 'weather_analytics'
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = 'clickhouse123'

def run_command(cmd, description):
    """Run shell command and handle errors"""
    print(f"[*] {description}...")
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"    [OK] Success")
            return True
        else:
            print(f"    [X] Error: {result.stderr}")
            return False
    except Exception as e:
        print(f"    [X] Exception: {str(e)}")
        return False

def execute_clickhouse_query(query):
    """Execute a query on ClickHouse"""
    url = f'http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}'
    params = {'database': CLICKHOUSE_DB}
    auth = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD) if CLICKHOUSE_USER else None

    try:
        response = requests.post(url, params=params, auth=auth, data=query.encode('utf-8'))
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"    [X] ClickHouse Error: {e}")
        return None

def get_last_updated_time(table_name):
    """Get last updated time from ClickHouse metadata table"""
    print(f"[*] Checking last update time for {table_name}...")

    cmd = f'docker exec clickhouse clickhouse-client --user={CLICKHOUSE_USER} --password={CLICKHOUSE_PASSWORD} --query="SELECT last_updated_time FROM meta.updated_time WHERE table_name = \'{table_name}\' LIMIT 1"'
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

    if result.returncode == 0 and result.stdout.strip():
        last_time = result.stdout.strip()
        print(f"    [OK] Last updated: {last_time}")
        return last_time
    else:
        print(f"    [X] No metadata found, loading all folders")
        return None

def get_new_output_folders(last_updated_time=None):
    """Find all Hive output folders newer than last updated time"""
    print("[*] Finding new Hive analysis outputs...")

    # Get all folders with timestamps for top_10_temperate_cities
    cmd = 'docker exec namenode bash -c "hdfs dfs -ls /user/data/hive_output/ | grep top_10_temperate_cities_"'
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

    if result.returncode != 0 or not result.stdout.strip():
        print("    [X] No folders found")
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
                folders.append(timestamp_str)

    if folders:
        print(f"    [OK] Found {len(folders)} new folder(s): {', '.join(folders)}")
    else:
        print("    [X] No new folders found")

    return folders

def update_metadata_timestamp(table_name, timestamp):
    """Update ClickHouse metadata with latest timestamp for a table"""
    print(f"\n[*] Updating metadata for {table_name}...")

    # Delete old entry
    cmd_delete = f'docker exec clickhouse clickhouse-client --user={CLICKHOUSE_USER} --password={CLICKHOUSE_PASSWORD} --query="ALTER TABLE meta.updated_time DELETE WHERE table_name = \'{table_name}\'"'

    # Insert new entry
    cmd_insert = f'docker exec clickhouse clickhouse-client --user={CLICKHOUSE_USER} --password={CLICKHOUSE_PASSWORD} --query="INSERT INTO meta.updated_time (table_name, last_updated_time) VALUES (\'{table_name}\', \'{timestamp}\')"'

    if run_command(cmd_delete, "  Deleting old metadata entry"):
        if run_command(cmd_insert, "  Inserting new metadata entry"):
            print(f"    [OK] Metadata updated successfully")
        else:
            print(f"    [X] Failed to insert new metadata")
    else:
        print(f"    [X] Failed to delete old metadata")

def load_top_temperate_cities(timestamps):
    """Load top temperate cities data from HDFS to ClickHouse"""
    print("\n" + "="*60)
    print("Loading Top Temperate Cities Data")
    print("="*60 + "\n")

    if not timestamps:
        print("\n[*] No new data to load")
        return False

    # Create hive_results directory if it doesn't exist
    Path("./hive_results").mkdir(exist_ok=True)

    total_records = 0

    for timestamp_str in timestamps:
        print(f"\n[*] Processing timestamp: {timestamp_str}")
        hdfs_path = f"/user/data/hive_output/top_10_temperate_cities_{timestamp_str}/000000_0"
        local_filename = f"top_10_temperate_cities_{timestamp_str}.csv"

        # Convert timestamp to datetime format for ClickHouse
        # From: "20251115183603" to "2025-11-15 18:36:03"
        datetime_str = f"{timestamp_str[0:4]}-{timestamp_str[4:6]}-{timestamp_str[6:8]} {timestamp_str[8:10]}:{timestamp_str[10:12]}:{timestamp_str[12:14]}"

        # Step 1: Copy from HDFS to container's /tmp
        cmd1 = f'docker exec namenode bash -c "hdfs dfs -get -f {hdfs_path} /tmp/query1.csv"'
        if run_command(cmd1, f"  Copying from HDFS"):
            # Step 2: Copy from container to local machine
            cmd2 = f"docker cp namenode:/tmp/query1.csv ./hive_results/{local_filename}"
            run_command(cmd2, f"  Saving to hive_results/{local_filename}")

            # Step 3: Load into ClickHouse
            try:
                # Get data from HDFS
                cmd = ['docker', 'exec', 'namenode', 'bash', '-c', f'hdfs dfs -cat {hdfs_path}']
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)

                # Parse CSV data and insert into ClickHouse
                lines = result.stdout.strip().split('\n')
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
                        VALUES ('{city_name}', {avg_temp}, '{datetime_str}')
                        """
                        if execute_clickhouse_query(insert_query):
                            records_inserted += 1

                print(f"    [OK] Inserted {records_inserted} records from {timestamp_str}")
                total_records += records_inserted

            except Exception as e:
                print(f"    [X] Error loading {timestamp_str}: {str(e)}")

    if total_records > 0:
        # Find the latest timestamp from all loaded folders
        latest_timestamp = max(timestamps)
        # Convert to datetime format
        datetime_str = f"{latest_timestamp[0:4]}-{latest_timestamp[4:6]}-{latest_timestamp[6:8]} {latest_timestamp[8:10]}:{latest_timestamp[10:12]}:{latest_timestamp[12:14]}"

        # Update metadata with latest timestamp
        update_metadata_timestamp('top_temperate_cities', datetime_str)

        print(f"\n[*] Total records loaded: {total_records}")
        return True
    else:
        return False

def load_evapotranspiration_by_season(timestamps):
    """Load evapotranspiration by season data from HDFS to ClickHouse"""
    print("\n" + "="*60)
    print("Loading Evapotranspiration by Season Data")
    print("="*60 + "\n")

    if not timestamps:
        print("\n[*] No new data to load")
        return False

    # Create hive_results directory if it doesn't exist
    Path("./hive_results").mkdir(exist_ok=True)

    total_records = 0

    for timestamp_str in timestamps:
        print(f"\n[*] Processing timestamp: {timestamp_str}")
        hdfs_path = f"/user/data/hive_output/avg_evapotranspiration_by_season_{timestamp_str}/000000_0"
        local_filename = f"avg_evapotranspiration_by_season_{timestamp_str}.csv"

        # Convert timestamp to datetime format for ClickHouse
        datetime_str = f"{timestamp_str[0:4]}-{timestamp_str[4:6]}-{timestamp_str[6:8]} {timestamp_str[8:10]}:{timestamp_str[10:12]}:{timestamp_str[12:14]}"

        # Step 1: Copy from HDFS to container's /tmp
        cmd1 = f'docker exec namenode bash -c "hdfs dfs -get -f {hdfs_path} /tmp/query2.csv"'
        if run_command(cmd1, f"  Copying from HDFS"):
            # Step 2: Copy from container to local machine
            cmd2 = f"docker cp namenode:/tmp/query2.csv ./hive_results/{local_filename}"
            run_command(cmd2, f"  Saving to hive_results/{local_filename}")

            # Step 3: Load into ClickHouse
            try:
                # Get data from HDFS
                cmd = ['docker', 'exec', 'namenode', 'bash', '-c', f'hdfs dfs -cat {hdfs_path}']
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)

                # Parse CSV data and insert into ClickHouse
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
                        VALUES ('{city_name}', '{season}', {year}, {avg_et}, '{datetime_str}')
                        """
                        if execute_clickhouse_query(insert_query):
                            records_inserted += 1

                print(f"    [OK] Inserted {records_inserted} records from {timestamp_str}")
                total_records += records_inserted

            except Exception as e:
                print(f"    [X] Error loading {timestamp_str}: {str(e)}")

    if total_records > 0:
        # Find the latest timestamp from all loaded folders
        latest_timestamp = max(timestamps)
        # Convert to datetime format
        datetime_str = f"{latest_timestamp[0:4]}-{latest_timestamp[4:6]}-{latest_timestamp[6:8]} {latest_timestamp[8:10]}:{latest_timestamp[10:12]}:{latest_timestamp[12:14]}"

        # Update metadata with latest timestamp
        update_metadata_timestamp('evapotranspiration_by_season', datetime_str)

        print(f"\n[*] Total records loaded: {total_records}")
        return True
    else:
        return False

def show_clickhouse_summary():
    """Show summary of data in ClickHouse"""
    print("\n" + "="*60)
    print("ClickHouse Data Warehouse Summary")
    print("="*60)

    # Get record counts
    count_query1 = "SELECT count() FROM top_temperate_cities"
    count_query2 = "SELECT count() FROM evapotranspiration_by_season"

    count1 = execute_clickhouse_query(count_query1)
    count2 = execute_clickhouse_query(count_query2)

    if count1 and count2:
        print(f"\n[*] Total records in top_temperate_cities: {count1.strip()}")
        print(f"[*] Total records in evapotranspiration_by_season: {count2.strip()}")

    # Display last updated times
    print("\n" + "-"*60)
    print("Last Updated Times (from metadata)")
    print("-"*60)
    last_update_1 = get_last_updated_time('top_temperate_cities')
    last_update_2 = get_last_updated_time('evapotranspiration_by_season')

    print(f"\n[*] ClickHouse Web UI: http://localhost:8123/play")
    print(f"[*] Database: {CLICKHOUSE_DB}")

def main():
    """Main execution function"""
    print("\n" + "="*60)
    print("Hive Results Loader - Load from HDFS to ClickHouse")
    print("="*60)

    # Get last updated time for top_temperate_cities
    last_updated_time = get_last_updated_time('top_temperate_cities')

    # Find all new folders (newer than last update)
    new_folders = get_new_output_folders(last_updated_time)

    if new_folders:
        # Load top temperate cities data
        load_top_temperate_cities(new_folders)

        # Load evapotranspiration data (same timestamps)
        load_evapotranspiration_by_season(new_folders)

        print("\n" + "="*60)
        print("Data Loading Complete!")
        print("="*60)
        print(f"\nFiles saved to hive_results/ folder")
    else:
        print("\n[*] No new data to load. All existing data is already up to date.")

    # Show summary
    show_clickhouse_summary()

if __name__ == "__main__":
    main()
