"""
TASK 2:
1. Extract data from the database
2. Transform the data:
    - Calculate the total transaction amount per user.
    - Identify the top 10 users by transaction volume.
    - Aggregate daily transaction totals across all users.
3. Loads the processed data back into the database or prepares it for use by the API.
"""

import sqlite3
import pandas as pd

DATABASE_PATH = "transactions_data.db"


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
        raise ValueError(f"An error occurred while executing the query: {e}")

def calculate_total_transaction_amount_per_user():
    """

    :return:
    """

    query = """
    SELECT 
        u.user_id, 
        SUM(t.amount) AS total_transaction_amount
    FROM 
        users u
    JOIN 
        transactions t
    ON 
        u.user_id = t.user_id
    GROUP BY 
        u.user_id, u.country
    ORDER BY 
        u.user_id ASC;
    """
    # NOTE: LEFT JOIN IS USED TO INCLUDE ALL USERS EVEN IF THEY HAVE NO TRANSACTIONS

    result = execute_custom_query(query)
    print(f'\nTotal Transaction Amount Per User:')
    print(result)
    return result

def identify_top_ten_users_by_transaction_volume():
    """

    :return:
    """

    query = """
        SELECT 
            u.user_id,
            COUNT(t.transaction_id) AS transaction_volume
        FROM 
            users u
        LEFT JOIN 
            transactions t
        ON 
            u.user_id = t.user_id
        GROUP BY 
            u.user_id, u.country
        ORDER BY 
            transaction_volume DESC
        LIMIT 10;
    """
    # NOTE: LEFT JOIN IS USED TO INCLUDE ALL USERS EVEN IF THEY HAVE NO TRANSACTIONS

    result = execute_custom_query(query)
    print(f'\nTop Ten Users by Transaction Volume: ')
    print(result)
    return result

def aggregate_daily_transactions():
    """

    :return:
    """

    query = """
        SELECT 
            t.transaction_date, 
            t.transaction_type,
            SUM(t.amount) AS daily_total
        FROM 
            transactions t
        GROUP BY 
            t.transaction_date, t.transaction_type
        ORDER BY 
            t.transaction_date, t.transaction_type;
    """
    # NOTE: LEFT JOIN IS USED TO INCLUDE ALL USERS EVEN IF THEY HAVE NO TRANSACTIONS

    result = execute_custom_query(query)
    print(f'\nDaily Aggregates Per Transaction Type')
    print(result)
    return result

def etl_executive():

    calculate_total_transaction_amount_per_user()
    identify_top_ten_users_by_transaction_volume()
    aggregate_daily_transactions()


