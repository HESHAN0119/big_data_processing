"""
Database connection utilities for ClickHouse
"""

from clickhouse_driver import Client
import os
import pandas as pd

# ClickHouse connection settings from environment variables
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', 9000))
CLICKHOUSE_DB = os.getenv('CLICKHOUSE_DATABASE', 'weather_analytics')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')

def get_clickhouse_client():
    """
    Create and return a ClickHouse client connection

    Returns:
        Client: ClickHouse client instance
    """
    try:
        client = Client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            database=CLICKHOUSE_DB,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD
        )
        return client
    except Exception as e:
        print(f"Error connecting to ClickHouse: {e}")
        raise

def execute_query(query, as_dataframe=True):
    """
    Execute a SQL query and return results

    Args:
        query (str): SQL query to execute
        as_dataframe (bool): Return results as pandas DataFrame if True

    Returns:
        DataFrame or list: Query results
    """
    try:
        client = get_clickhouse_client()

        if as_dataframe:
            # Execute query with column names
            result, columns_info = client.execute(query, with_column_types=True)

            if not result or len(result) == 0:
                return pd.DataFrame()

            column_names = [col[0] for col in columns_info]

            # Create DataFrame
            df = pd.DataFrame(result, columns=column_names)
            return df
        else:
            # Return raw results
            result = client.execute(query)
            return result

    except Exception as e:
        print(f"Error executing query: {e}")
        print(f"Query: {query}")
        raise

def read_sql_file(filepath):
    """
    Read SQL query from file

    Args:
        filepath (str): Path to SQL file

    Returns:
        str: SQL query content
    """
    try:
        with open(filepath, 'r') as f:
            sql = f.read()
        return sql
    except Exception as e:
        print(f"Error reading SQL file {filepath}: {e}")
        raise

def test_connection():
    """
    Test ClickHouse connection

    Returns:
        bool: True if connection successful
    """
    try:
        client = get_clickhouse_client()
        result = client.execute("SELECT 1")
        return result[0][0] == 1
    except Exception as e:
        print(f"Connection test failed: {e}")
        return False

def get_table_info(table_name):
    """
    Get information about a ClickHouse table

    Args:
        table_name (str): Name of the table

    Returns:
        dict: Table statistics
    """
    try:
        client = get_clickhouse_client()

        # Get row count
        count_query = f"SELECT count() as count FROM {table_name}"
        count_result = client.execute(count_query)
        row_count = count_result[0][0]

        # Get column info
        desc_query = f"DESCRIBE TABLE {table_name}"
        columns = client.execute(desc_query)

        return {
            'table_name': table_name,
            'row_count': row_count,
            'columns': columns
        }
    except Exception as e:
        print(f"Error getting table info for {table_name}: {e}")
        return None

# Month names mapping
MONTH_NAMES = {
    1: 'January', 2: 'February', 3: 'March', 4: 'April',
    5: 'May', 6: 'June', 7: 'July', 8: 'August',
    9: 'September', 10: 'October', 11: 'November', 12: 'December'
}

def get_month_name(month_num):
    """Get month name from number"""
    return MONTH_NAMES.get(month_num, f"Month {month_num}")
