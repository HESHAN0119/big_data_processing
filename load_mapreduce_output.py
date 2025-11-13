"""
Load MapReduce Output from HDFS to Pandas DataFrame
Automatically copies data from HDFS and loads into Python
"""

import subprocess
import pandas as pd
import os
from pathlib import Path
import time
import requests
import tempfile

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

def get_last_updated_time():
    """Get last updated time from ClickHouse metadata table"""
    print("[*] Checking last update time from ClickHouse metadata...")

    # Query ClickHouse metadata table
    cmd = 'docker exec clickhouse clickhouse-client --user=default --password=clickhouse123 --query="SELECT last_updated_time FROM meta.updated_time WHERE table_name = \'district_monthly_weather\' LIMIT 1"'
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

    if result.returncode == 0 and result.stdout.strip():
        last_time = result.stdout.strip()
        print(f"    [OK] Last updated: {last_time}")
        return last_time
    else:
        print("    [X] No metadata found, loading all folders")
        return None

def get_new_output_folders(last_updated_time=None):
    """Find all output folders newer than last updated time"""
    print("[*] Finding new MapReduce outputs...")

    # Get all folders with timestamps
    cmd = 'docker exec namenode bash -c "hdfs dfs -ls /user/data/mapreduce_output/district_monthly/ | grep mean_temp_"'
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

    if result.returncode != 0 or not result.stdout.strip():
        print("    [X] No folders found")
        return []

    folders = []
    for line in result.stdout.strip().split('\n'):
        if 'mean_temp_' in line:
            folder_path = line.split()[-1]
            folder_name = folder_path.split('/')[-1]
            # Extract timestamp from folder name: mean_temp_20251113061035
            timestamp_str = folder_name.replace('mean_temp_', '')

            # If we have a last updated time, filter by it
            if last_updated_time:
                # Convert datetime string to comparable format
                # From: "2025-11-13 06:46:50" to "20251113064650"
                last_time_formatted = last_updated_time.replace('-', '').replace(' ', '').replace(':', '')

                # Compare timestamps (format: YYYYMMDDHHmmss)
                print(f"    Comparing: {timestamp_str} vs {last_time_formatted}")
                if timestamp_str > last_time_formatted:
                    print(f"      -> Including {folder_name} (newer)")
                    folders.append(folder_name)
                else:
                    print(f"      -> Skipping {folder_name} (older or same)")
            else:
                folders.append(folder_name)

    if folders:
        print(f"    [OK] Found {len(folders)} new folder(s): {', '.join(folders)}")
    else:
        print("    [X] No new folders found")

    return folders

def update_metadata_timestamp(folder_names):
    """Update ClickHouse metadata with latest folder timestamp"""
    if not folder_names:
        return

    # Find the latest timestamp from all loaded folders
    timestamps = [folder.replace('mean_temp_', '') for folder in folder_names]
    latest_timestamp = max(timestamps)  # Format: YYYYMMDDHHmmss

    # Convert to datetime format: YYYY-MM-DD HH:mm:ss
    datetime_str = f"{latest_timestamp[0:4]}-{latest_timestamp[4:6]}-{latest_timestamp[6:8]} {latest_timestamp[8:10]}:{latest_timestamp[10:12]}:{latest_timestamp[12:14]}"

    print(f"\n[*] Updating metadata with latest timestamp: {datetime_str}")

    # Delete old entry and insert new one
    cmd_delete = 'docker exec clickhouse clickhouse-client --user=default --password=clickhouse123 --query="ALTER TABLE meta.updated_time DELETE WHERE table_name = \'district_monthly_weather\'"'
    cmd_insert = f'docker exec clickhouse clickhouse-client --user=default --password=clickhouse123 --query="INSERT INTO meta.updated_time (table_name, last_updated_time) VALUES (\'district_monthly_weather\', \'{datetime_str}\')"'

    if run_command(cmd_delete, "  Deleting old metadata entry"):
        if run_command(cmd_insert, "  Inserting new metadata entry"):
            print(f"    [OK] Metadata updated successfully")
        else:
            print(f"    [X] Failed to insert new metadata")
    else:
        print(f"    [X] Failed to delete old metadata")

def insert_dataframe_to_clickhouse(df):
    """Insert DataFrame into ClickHouse weather_analytics.district_monthly_weather table"""
    if df is None or len(df) == 0:
        print("    [X] No data to insert")
        return False

    print(f"\n[*] Inserting {len(df)} records into ClickHouse...")

    try:
        # Create a temporary TSV file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.tsv', newline='') as tmp_file:
            tmp_path = tmp_file.name

            # Write data in TSV format (district, year, month, year_month, total_precipitation_hours, mean_temperature)
            for _, row in df.iterrows():
                line = f"{row['district']}\t{row['year']}\t{row['month']}\t{row['year_month']}\t{row['total_precipitation_hours']}\t{row['mean_temperature']}\n"
                tmp_file.write(line)

        # Insert into ClickHouse using HTTP interface
        with open(tmp_path, 'rb') as f:
            url = f'http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}'
            params = {
                'database': CLICKHOUSE_DB,
                'query': 'INSERT INTO district_monthly_weather (district, year, month, year_month, total_precipitation_hours, mean_temperature) FORMAT TabSeparated'
            }
            auth = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD) if CLICKHOUSE_PASSWORD else None
            response = requests.post(url, params=params, auth=auth, data=f)
            response.raise_for_status()

        print(f"    [OK] Successfully inserted {len(df)} records into ClickHouse")

        # Verify insertion by counting records
        verify_url = f'http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}'
        verify_params = {
            'database': CLICKHOUSE_DB,
            'query': 'SELECT count() FROM district_monthly_weather'
        }
        auth = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD) if CLICKHOUSE_PASSWORD else None
        verify_response = requests.post(verify_url, params=verify_params, auth=auth)
        total_count = verify_response.text.strip()
        print(f"    [OK] Total records in ClickHouse table: {total_count}")

        # Clean up temp file
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)

        return True

    except requests.exceptions.RequestException as e:
        print(f"    [X] Error inserting into ClickHouse: {e}")
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        return False
    except Exception as e:
        print(f"    [X] Unexpected error: {e}")
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        return False

def load_district_monthly_weather():
    """Load district monthly weather data from HDFS based on metadata timestamp"""
    print("\n" + "="*60)
    print("Loading District Monthly Weather Data from HDFS")
    print("="*60 + "\n")

    # Create hdfs_results directory if it doesn't exist
    Path("./hdfs_results").mkdir(exist_ok=True)

    # Get last updated time from ClickHouse metadata
    last_updated_time = get_last_updated_time()

    # Find all new folders (newer than last update)
    new_folders = get_new_output_folders(last_updated_time)

    if not new_folders:
        print("\n[*] No new data to load")
        return None

    # Load data from all new folders
    all_dataframes = []

    for folder_name in new_folders:
        print(f"\n[*] Processing folder: {folder_name}")
        hdfs_path = f"/user/data/mapreduce_output/district_monthly/{folder_name}/part-r-00000"
        local_filename = f"{folder_name}.csv"

        # Step 1: Copy from HDFS to container's /tmp (force overwrite with -f)
        cmd1 = f'docker exec namenode bash -c "hdfs dfs -get -f {hdfs_path} /tmp/district_monthly.tsv"'
        if run_command(cmd1, f"  Copying from HDFS"):
            # Step 2: Copy from container to local machine
            cmd2 = f"docker cp namenode:/tmp/district_monthly.tsv ./hdfs_results/{local_filename}"
            run_command(cmd2, f"  Saving to hdfs_results/{local_filename}")

            # Step 3: Load into Pandas
            try:
                df = pd.read_csv(
                    f'hdfs_results/{local_filename}',
                    sep='\t',
                    header=None,
                    names=['district', 'year_month', 'total_precipitation_hours', 'mean_temperature']
                )
                print(f"    [OK] Loaded {len(df)} records from {folder_name}")

                # Add year and month columns for easier analysis
                df[['year', 'month']] = df['year_month'].str.split('-', expand=True)
                df['year'] = df['year'].astype(int)
                df['month'] = df['month'].astype(int)

                all_dataframes.append(df)
            except Exception as e:
                print(f"    [X] Error loading {folder_name}: {str(e)}")

    if all_dataframes:
        # Combine all dataframes
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        print(f"\n[*] Total records loaded: {len(combined_df)}")

        # Insert new data into ClickHouse
        if insert_dataframe_to_clickhouse(combined_df):
            # Update metadata with latest timestamp only if insertion was successful
            update_metadata_timestamp(new_folders)
        else:
            print("    [\!] Warning: Failed to insert data into ClickHouse, metadata not updated")

        return combined_df
    else:
        return None

def get_last_updated_time_hp():
    """Get last updated time for highest_precipitation from ClickHouse metadata table"""
    print("[*] Checking last update time from ClickHouse metadata...")

    # Query ClickHouse metadata table
    cmd = 'docker exec clickhouse clickhouse-client --user=default --password=clickhouse123 --query="SELECT last_updated_time FROM meta.updated_time WHERE table_name = \'highest_precipitation\' LIMIT 1"'
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

    if result.returncode == 0 and result.stdout.strip():
        last_time = result.stdout.strip()
        print(f"    [OK] Last updated: {last_time}")
        return last_time
    else:
        print("    [X] No metadata found, loading all folders")
        return None

def get_new_hp_folders(last_updated_time=None):
    """Find all highest precipitation output folders newer than last updated time"""
    print("[*] Finding new highest precipitation outputs...")

    # Get all folders with timestamps
    cmd = 'docker exec namenode bash -c "hdfs dfs -ls /user/data/mapreduce_output/highest_precipitation/ | grep hpm_"'
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

    if result.returncode != 0 or not result.stdout.strip():
        print("    [X] No folders found")
        return []

    folders = []
    for line in result.stdout.strip().split('\n'):
        if 'hpm_' in line:
            folder_path = line.split()[-1]
            folder_name = folder_path.split('/')[-1]
            # Extract timestamp from folder name: hpm_20251113113647
            timestamp_str = folder_name.replace('hpm_', '')

            # If we have a last updated time, filter by it
            if last_updated_time:
                # Convert datetime string to comparable format
                # From: "2025-11-13 11:36:47" to "20251113113647"
                last_time_formatted = last_updated_time.replace('-', '').replace(' ', '').replace(':', '')

                # Compare timestamps (format: YYYYMMDDHHmmss)
                print(f"    Comparing: {timestamp_str} vs {last_time_formatted}")
                if timestamp_str > last_time_formatted:
                    print(f"      -> Including {folder_name} (newer)")
                    folders.append(folder_name)
                else:
                    print(f"      -> Skipping {folder_name} (older or same)")
            else:
                folders.append(folder_name)

    if folders:
        print(f"    [OK] Found {len(folders)} new folder(s): {', '.join(folders)}")
    else:
        print("    [X] No new folders found")

    return folders

def update_metadata_timestamp_hp(folder_names):
    """Update ClickHouse metadata with latest highest_precipitation folder timestamp"""
    if not folder_names:
        return

    # Find the latest timestamp from all loaded folders
    timestamps = [folder.replace('hpm_', '') for folder in folder_names]
    latest_timestamp = max(timestamps)  # Format: YYYYMMDDHHmmss

    # Convert to datetime format: YYYY-MM-DD HH:mm:ss
    datetime_str = f"{latest_timestamp[0:4]}-{latest_timestamp[4:6]}-{latest_timestamp[6:8]} {latest_timestamp[8:10]}:{latest_timestamp[10:12]}:{latest_timestamp[12:14]}"

    print(f"\n[*] Updating metadata with latest timestamp: {datetime_str}")

    # Delete old entry and insert new one
    cmd_delete = 'docker exec clickhouse clickhouse-client --user=default --password=clickhouse123 --query="ALTER TABLE meta.updated_time DELETE WHERE table_name = \'highest_precipitation\'"'
    cmd_insert = f'docker exec clickhouse clickhouse-client --user=default --password=clickhouse123 --query="INSERT INTO meta.updated_time (table_name, last_updated_time) VALUES (\'highest_precipitation\', \'{datetime_str}\')"'

    if run_command(cmd_delete, "  Deleting old metadata entry"):
        if run_command(cmd_insert, "  Inserting new metadata entry"):
            print(f"    [OK] Metadata updated successfully")
        else:
            print(f"    [X] Failed to insert new metadata")
    else:
        print(f"    [X] Failed to delete old metadata")

def insert_hp_to_clickhouse(df):
    """Insert highest precipitation DataFrame into ClickHouse weather_analytics.highest_precipitation table"""
    if df is None or len(df) == 0:
        print("    [X] No data to insert")
        return False

    print(f"\n[*] Inserting {len(df)} record(s) into ClickHouse highest_precipitation table...")

    try:
        # Create a temporary TSV file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.tsv', newline='') as tmp_file:
            tmp_path = tmp_file.name

            # Write data in TSV format (year_month, total_precipitation_hours)
            for _, row in df.iterrows():
                # Split year_month into year and month
                year, month = row['year_month'].split('-')
                line = f"{row['year_month']}\t{year}\t{month}\t{row['total_precipitation_hours']}\n"
                tmp_file.write(line)

        # Insert into ClickHouse using HTTP interface
        with open(tmp_path, 'rb') as f:
            url = f'http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}'
            params = {
                'database': CLICKHOUSE_DB,
                'query': 'INSERT INTO highest_precipitation (year_month, year, month, total_precipitation_hours) FORMAT TabSeparated'
            }
            auth = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD) if CLICKHOUSE_PASSWORD else None
            response = requests.post(url, params=params, auth=auth, data=f)
            response.raise_for_status()

        print(f"    [OK] Successfully inserted {len(df)} record(s) into ClickHouse")

        # Verify insertion by counting records
        verify_url = f'http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}'
        verify_params = {
            'database': CLICKHOUSE_DB,
            'query': 'SELECT count() FROM highest_precipitation'
        }
        auth = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD) if CLICKHOUSE_PASSWORD else None
        verify_response = requests.post(verify_url, params=verify_params, auth=auth)
        total_count = verify_response.text.strip()
        print(f"    [OK] Total records in ClickHouse table: {total_count}")

        # Clean up temp file
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)

        return True

    except requests.exceptions.RequestException as e:
        print(f"    [X] Error inserting into ClickHouse: {e}")
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        return False
    except Exception as e:
        print(f"    [X] Unexpected error: {e}")
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        return False

def load_highest_precipitation():
    """Load highest precipitation month data from HDFS based on metadata timestamp"""
    print("\n" + "="*60)
    print("Loading Highest Precipitation Data from HDFS")
    print("="*60 + "\n")

    # Create hdfs_results directory if it doesn't exist
    Path("./hdfs_results").mkdir(exist_ok=True)

    # Get last updated time from ClickHouse metadata
    last_updated_time = get_last_updated_time_hp()

    # Find all new folders (newer than last update)
    new_folders = get_new_hp_folders(last_updated_time)

    if not new_folders:
        print("\n[*] No new data to load")
        return None

    # Load data from all new folders
    all_dataframes = []

    for folder_name in new_folders:
        print(f"\n[*] Processing folder: {folder_name}")
        hdfs_path = f"/user/data/mapreduce_output/highest_precipitation/{folder_name}/part-r-00000"
        local_filename = f"{folder_name}.csv"

        # Step 1: Copy from HDFS to container's /tmp (force overwrite with -f)
        cmd1 = f'docker exec namenode bash -c "hdfs dfs -get -f {hdfs_path} /tmp/highest_precipitation.tsv"'
        if run_command(cmd1, f"  Copying from HDFS"):
            # Step 2: Copy from container to local machine
            cmd2 = f"docker cp namenode:/tmp/highest_precipitation.tsv ./hdfs_results/{local_filename}"
            run_command(cmd2, f"  Saving to hdfs_results/{local_filename}")

            # Step 3: Load into Pandas
            try:
                df = pd.read_csv(
                    f'hdfs_results/{local_filename}',
                    sep='\t',
                    header=None,
                    names=['year_month', 'total_precipitation_hours']
                )
                print(f"    [OK] Loaded {len(df)} record(s) from {folder_name}")
                all_dataframes.append(df)
            except Exception as e:
                print(f"    [X] Error loading {folder_name}: {str(e)}")

    if all_dataframes:
        # Combine all dataframes
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        print(f"\n[*] Total records loaded: {len(combined_df)}")

        # Insert new data into ClickHouse
        if insert_hp_to_clickhouse(combined_df):
            # Update metadata with latest timestamp only if insertion was successful
            update_metadata_timestamp_hp(new_folders)
        else:
            print("    [\!] Warning: Failed to insert data into ClickHouse, metadata not updated")

        return combined_df
    else:
        return None

def show_statistics(df):
    """Display data statistics and sample records"""
    print("\n" + "="*60)
    print("Data Statistics")
    print("="*60 + "\n")

    print(f"Total Records: {len(df)}")
    print(f"Date Range: {df['year'].min()}-{df['year'].max()}")
    print(f"Districts: {df['district'].nunique()}")
    print(f"\nDistrict List:\n{df['district'].unique()}")

    print("\n" + "-"*60)
    print("Precipitation Statistics (hours)")
    print("-"*60)
    print(df['total_precipitation_hours'].describe())

    print("\n" + "-"*60)
    print("Temperature Statistics (°C)")
    print("-"*60)
    print(df['mean_temperature'].describe())

    print("\n" + "-"*60)
    print("Sample Records (First 10)")
    print("-"*60)
    print(df.head(10).to_string(index=False))

    print("\n" + "-"*60)
    print("Top 10 Wettest Months")
    print("-"*60)
    top_precip = df.nlargest(10, 'total_precipitation_hours')[
        ['district', 'year_month', 'total_precipitation_hours', 'mean_temperature']
    ]
    print(top_precip.to_string(index=False))

def save_to_csv(df, filename):
    """Save DataFrame to CSV file"""
    print(f"\n[*] Saving data to {filename}...")
    try:
        df.to_csv(filename, index=False)
        print(f"    [OK] Saved successfully")
        print(f"    File size: {os.path.getsize(filename)} bytes")
    except Exception as e:
        print(f"    [X] Error saving: {str(e)}")

def main():
    """Main execution function"""
    print("\n" + "="*60)
    print("MapReduce Output Loader")
    print("="*60)

    # Load district monthly weather data
    df_weather = load_district_monthly_weather()

    if df_weather is not None:
        # Show statistics
        show_statistics(df_weather)

        # Save to clean CSV
        # save_to_csv(df_weather, 'data/district_monthly_weather_clean.csv')

        print("\n" + "="*60)
        print("Data Loading Complete!")
        print("="*60)
        print(f"\nFiles saved to hdfs_results/ folder:")
        print(f"  - Latest: hdfs_results/mean_temp_YYYYMMDDHHMMSS.csv")
        print(f"  - Highest precipitation: hdfs_results/highest_precipitation.csv")
        print(f"\nYou can now use the data in Python:")
        print(f"  >>> import pandas as pd")
        print(f"  >>> df = pd.read_csv('hdfs_results/mean_temp_20251113061035.csv')")
        print(f"  >>> df.head()")

        # Also load highest precipitation
        print("\n")
        df_highest = load_highest_precipitation()
        if df_highest is not None:
            print(f"\n[*] Highest Precipitation Month:")
            print(f"    {df_highest['year_month'].values[0]}: {df_highest['total_precipitation_hours'].values[0]:.2f} hours")
    else:
        print("\n[X] Failed to load data. Please check:")
        print("  1. Docker containers are running (docker ps)")
        print("  2. MapReduce jobs completed successfully")
        print("  3. HDFS contains the output files")

if __name__ == "__main__":
    main()
