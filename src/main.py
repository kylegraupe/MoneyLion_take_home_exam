"""
Execution endpoint.
"""

import sqlite3
import pandas as pd

import import_raw_to_db
import etl
import flask_api


DATABASE_PATH = "transactions_data.db"


def execute_custom_query(query):
    """
    Executes a custom SQL query on the 'users' table and returns the result.

    :param query: The SQL query string to execute (must involve the 'users' table).
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
        raise ValueError(f"An error occurred while executing the query: {e}")


if __name__ == "__main__":

    import_raw_to_db.data_import_executive()
    etl.etl_executive()
    flask_api.app.run(debug=True, use_reloader=False)

    # query = "select * from users"
    # print(execute_custom_query(query))