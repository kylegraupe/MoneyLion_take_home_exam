"""
TASK 2: ETL
1. Extract data from the database
2. Transform the data:
    1. Calculate the total transaction amount per user.
    2. Identify the top 10 users by transaction volume.
    3. Aggregate daily transaction totals across all users.
3. Loads the processed data back into the database or prepares it for use by the API.
"""

import sqlite3
import pandas as pd

import logs
import settings

DATABASE_PATH = settings.DB_PATH
pd.set_option('display.max_columns', None)


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

def calculate_total_transaction_amount_per_user(log_events=True):
    """
    This function calculates the total transaction amount as defined by the sum of all transactions from the associated
    user and the total transaction amounts for deposits, withdrawals, and purchases for the associated user.
    :return: Pandas DataFrame containing query results
    """

    query = """
    SELECT 
        u.user_id, 
        SUM(t.amount) AS total_transaction_amount,
        SUM(CASE WHEN t.transaction_type = 'deposit' THEN t.amount ELSE 0 END) AS total_deposit,
        SUM(CASE WHEN t.transaction_type = 'withdrawal' THEN t.amount ELSE 0 END) AS total_withdrawal,
        SUM(CASE WHEN t.transaction_type = 'purchase' THEN t.amount ELSE 0 END) AS total_purchase
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
    if log_events:
        logs.log_event(f'Task 2-2-1 Completed. Total Transaction Amount Per User Calculated.')
        # Since this function is used multiple times, we do not want redundant logging when called in the application.
        # Ordinarily, I would make sure that this function is only called once, but to demonstrate the completion of the
        # required tasks, I separated out the functionality.
    return result

def identify_top_ten_users_by_transaction_volume():
    """
    This function calculates the top users by transaction volume. Transaction volume is defined as the number of
    transactions for a given user.
    :return: Pandas DataFrame containing results
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
    logs.log_event(f'Task 2-2-2 Completed. Top Ten Users by Transaction Volume Calculated.')
    return result

def aggregate_daily_transactions():
    """
    This function aggregates the total deposits, purchases, and withdrawals made on each day in the dataset.
    :return: Pandas DataFrame containing result
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
    logs.log_event(f'Task 2-2-3 Completed. Daily Aggregates by Transaction Type Calculated.')
    return result

def alter_users_table_for_transaction_summary():
    """
    Alter the users table to add the columns needed for transaction summary data if they don't already exist.
    """
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    # Check if the columns already exist
    cursor.execute("PRAGMA table_info(users);")
    existing_columns = [column[1] for column in cursor.fetchall()]

    # Add the columns only if they do not already exist
    if 'total_transaction_amount' not in existing_columns:
        cursor.execute("""
            ALTER TABLE users
            ADD COLUMN total_transaction_amount REAL DEFAULT 0;
        """)
        print("Added column total_transaction_amount.")

    if 'total_deposit' not in existing_columns:
        cursor.execute("""
            ALTER TABLE users
            ADD COLUMN total_deposit REAL DEFAULT 0;
        """)
        print("Added column total_deposit.")

    if 'total_withdrawal' not in existing_columns:
        cursor.execute("""
            ALTER TABLE users
            ADD COLUMN total_withdrawal REAL DEFAULT 0;
        """)
        print("Added column total_withdrawal.")

    if 'total_purchase' not in existing_columns:
        cursor.execute("""
            ALTER TABLE users
            ADD COLUMN total_purchase REAL DEFAULT 0;
        """)
        print("Added column total_purchase.")

    conn.commit()
    conn.close()
    print("Users table updated successfully with transaction summary columns.")
    logs.log_event(f'Task 2-3 Completed. Users table updated successfully with transaction summary columns.')


def upsert_transaction_summary_to_users():
    """
    This function updates the user table with the calculated transaction summary
    without overwriting existing user data (signup_date, country).
    """
    transaction_summary = calculate_total_transaction_amount_per_user(log_events=False)

    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    # Iterate through the transaction summary rows and update each user in the users table
    for ix, row in transaction_summary.iterrows():
        user_id = row['user_id']
        total_transaction_amount = row['total_transaction_amount']
        total_deposit = row['total_deposit']
        total_withdrawal = row['total_withdrawal']
        total_purchase = row['total_purchase']

        # The query will update only the transaction summary columns
        query = """
        UPDATE users 
        SET 
            total_transaction_amount = ?,
            total_deposit = ?,
            total_withdrawal = ?,
            total_purchase = ?
        WHERE user_id = ?;
        """

        # Execute the update query
        cursor.execute(query, (total_transaction_amount, total_deposit, total_withdrawal, total_purchase, user_id))

    # Commit changes and close the connection
    conn.commit()
    conn.close()
    print("User transaction summary successfully updated without affecting existing user data.")
    logs.log_event(f'Task 2-3 Completed. User transaction summary successfully updated without affecting existing user data.')



def etl_executive():
    """
    Executive function that runs all ETL tasks in the appropriate order.
    :return: None
    """

    calculate_total_transaction_amount_per_user()
    identify_top_ten_users_by_transaction_volume()
    aggregate_daily_transactions()
    alter_users_table_for_transaction_summary()
    upsert_transaction_summary_to_users()
    logs.log_event(f'Task 2 Completed. All ETL Processes Completed.')
