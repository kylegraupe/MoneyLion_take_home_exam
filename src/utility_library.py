"""
Utility library for reusable code.
"""

import sqlite3
import pandas as pd

import settings
import logs

DATABASE_PATH = settings.DB_PATH

def execute_custom_query(query):
    """
    Executes a custom SQL query on the desired table and returns the result as a Pandas DataFrame.

    :param query: The SQL query string to execute.
    :return: Pandas DataFrame containing the query result.
    """
    conn = sqlite3.connect(DATABASE_PATH)
    try:
        # Execute the query and fetch the results
        result = pd.read_sql_query(query, conn)
        conn.close()
        return result
    except Exception as e:
        conn.close()
        logs.log_error(f'An error occurred while executing the query: {e}')
        raise ValueError(f"An error occurred while executing the query: {e}")

def query_db_for_api(query, params=()):
    """
    Function used to query SQLite Database
    :param query:
    :param params:
    :return:
    """
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row  # To access columns by name
    cur = conn.cursor()
    cur.execute(query, params)
    result = cur.fetchall()
    conn.close()
    return result