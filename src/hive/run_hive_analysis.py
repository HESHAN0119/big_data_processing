# """
# Run Hive Analysis Queries and Load Results
# Similar pattern to load_mapreduce_output.py
# """

# import subprocess
# import pandas as pd
# import os
# from pathlib import Path
# from datetime import datetime
# import requests
# import tempfile

# # ClickHouse Configuration
# CLICKHOUSE_HOST = 'localhost'
# CLICKHOUSE_PORT = 8123
# CLICKHOUSE_DB = 'weather_analytics'
# CLICKHOUSE_USER = 'default'
# CLICKHOUSE_PASSWORD = 'clickhouse123'

# # Hive queries configuration
# QUERIES = [
#     {
#         'name': 'top_10_temperate_cities',
#         'hql_file': './src/hive/02_query1_top_cities.hql',
#         'description': 'Top 10 Most Temperate Cities',
#         'table_name': 'top_10_temperate_cities',
#         'columns': ['city_name', 'avg_max_temp']
#     },
#     {
#         'name': 'avg_evapotranspiration_by_season',
#         'hql_file': './src/hive/03_query2_evapotranspiration.hql',
#         'description': 'Average Evapotranspiration by Agricultural Season',
#         'table_name': 'avg_evapotranspiration_by_season',
#         'columns': ['city_name', 'season', 'year', 'avg_evapotranspiration']
#     }
# ]

# def run_command(cmd, description):
#     """Run shell command and handle errors"""
#     print(f"[*] {description}...")
#     try:
#         result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
#         if result.returncode == 0:
#             print(f"    [OK] Success")
#             return True, result.stdout
#         else:
#             print(f"    [X] Error: {result.stderr}")
#             return False, result.stderr
#     except Exception as e:
#         print(f"    [X] Exception: {str(e)}")
#         return False, str(e)

# def execute_hive_query(query_config, timestamp):
#     """Execute Hive query and save results to HDFS with timestamp"""
#     print(f"\n{'='*60}")
#     print(f"Executing: {query_config['description']}")
#     print(f"{'='*60}\n")

#     query_name = query_config['name']
#     hql_file = query_config['hql_file']

#     # Read the HQL file
#     with open(hql_file, 'r') as f:
#         hql_content = f.read()

#     # Create HDFS output path with timestamp (like MapReduce pattern)
#     hdfs_output_dir = f"/user/data/hive_output/{query_name}_{timestamp}"

#     # Build complete HQL with INSERT OVERWRITE DIRECTORY
#     # This is similar to FileOutputFormat.setOutputPath in MapReduce
#     complete_hql = f"""
# USE weather_analytics;

# INSERT OVERWRITE DIRECTORY '{hdfs_output_dir}'
# ROW FORMAT DELIMITED
# FIELDS TERMINATED BY ','
# {hql_content.split('SELECT')[1]}
# """

#     # Write to temporary file
#     temp_hql_file = f"/tmp/hive_query_{query_name}_{timestamp}.hql"

#     # Copy HQL to namenode container
#     with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.hql') as tmp_file:
#         tmp_file.write(complete_hql)
#         local_temp_file = tmp_file.name

#     # Copy to container
#     copy_cmd = f"docker cp {local_temp_file} hive-server:{temp_hql_file}"
#     success, _ = run_command(copy_cmd, f"  Copying HQL script to hive-server")

#     if not success:
#         os.unlink(local_temp_file)
#         return None

#     # Execute Hive query using beeline (not deprecated hive CLI)
#     hive_cmd = f'docker exec hive-server /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -f {temp_hql_file}'
#     success, output = run_command(hive_cmd, f"  Running Hive query")

#     # Clean up temp files
#     os.unlink(local_temp_file)
#     subprocess.run(f'docker exec hive-server rm {temp_hql_file}', shell=True, capture_output=True)

#     if success:
#         print(f"    [OK] Results saved to HDFS: {hdfs_output_dir}")
#         return hdfs_output_dir
#     else:
#         return None

# def copy_results_from_hdfs(hdfs_path, local_filename):
#     """Copy results from HDFS to local hive_results folder (same as MapReduce pattern)"""
#     print(f"\n[*] Copying results from HDFS to local...")

#     # Create hive_results directory (separate from hdfs_results for MapReduce)
#     Path("./hive_results").mkdir(exist_ok=True)

#     # Step 1: Copy from HDFS to container's /tmp (force overwrite with -f)
#     # Same pattern as load_mapreduce_output.py line 208
#     temp_file = f"/tmp/hive_result_{local_filename}"
#     cmd1 = f'docker exec namenode bash -c "hdfs dfs -get -f {hdfs_path}/000000_0 {temp_file}"'

#     success, _ = run_command(cmd1, f"  Copying from HDFS to namenode:/tmp")
#     if not success:
#         return None

#     # Step 2: Copy from container to local machine
#     # Same pattern as load_mapreduce_output.py line 211
#     local_path = f"./hive_results/{local_filename}.csv"
#     cmd2 = f"docker cp namenode:{temp_file} {local_path}"
#     success, _ = run_command(cmd2, f"  Saving to {local_path}")

#     if success:
#         return local_path
#     else:
#         return None

# def load_results_to_dataframe(local_path, columns):
#     """Load CSV results into pandas DataFrame"""
#     print(f"[*] Loading results into DataFrame...")
#     try:
#         df = pd.read_csv(local_path, header=None, names=columns)
#         print(f"    [OK] Loaded {len(df)} records")
#         print(f"\n    Preview:")
#         print(f"    {'-'*60}")
#         print(df.head(10).to_string(index=False))
#         print(f"    {'-'*60}")
#         return df
#     except Exception as e:
#         print(f"    [X] Error loading data: {str(e)}")
#         return None

# def insert_to_clickhouse(df, table_name, query_config):
#     """Insert DataFrame into ClickHouse (optional - similar to MapReduce pattern)"""
#     if df is None or len(df) == 0:
#         print("    [X] No data to insert")
#         return False

#     print(f"\n[*] Inserting {len(df)} records into ClickHouse table: {table_name}...")

#     try:
#         # Create table if not exists
#         columns_def = []
#         for col in query_config['columns']:
#             if 'year' in col.lower():
#                 columns_def.append(f"{col} Int32")
#             elif 'temp' in col.lower() or 'avg' in col.lower() or 'evapotranspiration' in col.lower():
#                 columns_def.append(f"{col} Float64")
#             else:
#                 columns_def.append(f"{col} String")

#         create_table_query = f"""
#         CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.{table_name} (
#             {', '.join(columns_def)}
#         ) ENGINE = MergeTree()
#         ORDER BY tuple()
#         """

#         url = f'http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}'
#         params = {'query': create_table_query}
#         auth = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD) if CLICKHOUSE_PASSWORD else None
#         response = requests.post(url, params=params, auth=auth)
#         response.raise_for_status()
#         print(f"    [OK] Table {table_name} ready")

#         # Insert data using CSV format
#         insert_query = f'INSERT INTO {CLICKHOUSE_DB}.{table_name} FORMAT CSV'

#         # Convert DataFrame to CSV string
#         csv_data = df.to_csv(index=False, header=False)

#         params = {'query': insert_query}
#         response = requests.post(url, params=params, auth=auth, data=csv_data.encode('utf-8'))
#         response.raise_for_status()

#         print(f"    [OK] Successfully inserted {len(df)} records")

#         # Verify
#         verify_params = {
#             'database': CLICKHOUSE_DB,
#             'query': f'SELECT count() FROM {table_name}'
#         }
#         verify_response = requests.post(url, params=verify_params, auth=auth)
#         total_count = verify_response.text.strip()
#         print(f"    [OK] Total records in table: {total_count}")

#         return True

#     except Exception as e:
#         print(f"    [X] Error: {e}")
#         return False

# def main():
#     """Main execution function"""
#     print("\n" + "="*60)
#     print("Hive Analysis Pipeline")
#     print("="*60)

#     # Generate timestamp (same format as MapReduce: YYYYMMDDHHmmss)
#     timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
#     print(f"\nTimestamp: {timestamp}")

#     results_summary = []

#     # Execute each query
#     for query_config in QUERIES:
#         # Execute query and save to HDFS
#         hdfs_path = execute_hive_query(query_config, timestamp)

#         if hdfs_path:
#             # Copy results to local
#             local_filename = f"{query_config['name']}_{timestamp}"
#             local_path = copy_results_from_hdfs(hdfs_path, local_filename)

#             if local_path:
#                 # Load into DataFrame
#                 df = load_results_to_dataframe(local_path, query_config['columns'])

#                 if df is not None:
#                     # Optionally insert to ClickHouse
#                     insert_to_clickhouse(df, query_config['table_name'], query_config)

#                     results_summary.append({
#                         'query': query_config['description'],
#                         'hdfs_path': hdfs_path,
#                         'local_path': local_path,
#                         'records': len(df)
#                     })

#     # Print summary
#     print("\n" + "="*60)
#     print("Analysis Complete!")
#     print("="*60 + "\n")

#     if results_summary:
#         print("Results Summary:")
#         for result in results_summary:
#             print(f"\n  {result['query']}")
#             print(f"    - HDFS: {result['hdfs_path']}")
#             print(f"    - Local: {result['local_path']}")
#             print(f"    - Records: {result['records']}")

#         print(f"\n\nLocal results saved to: ./hive_results/")
#         print(f"  - {QUERIES[0]['name']}_{timestamp}.csv")
#         print(f"  - {QUERIES[1]['name']}_{timestamp}.csv")
#     else:
#         print("[X] No results generated. Please check:")
#         print("  1. Docker containers are running (docker ps)")
#         print("  2. Hive tables are created (run 01_create_tables.hql)")
#         print("  3. Data exists in HDFS (/user/data/kafka_ingested/)")

# if __name__ == "__main__":
#     main()
