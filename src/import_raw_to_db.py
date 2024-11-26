"""
TASK 1: Data Ingestion and Database Design
1. Design a relational database schema to store the data efficiently (Use OpenSource libraries
like SQLite).
    a. Design Schema for User Table
    b. Design Schema for Transaction Table
2. Implement a script to ingest the data from the CSV files into your database.
    a. Ingest Users CSV file into database.
    b. Ingest Transactions CSV file into database.
"""

import sqlite3
import pandas as pd

import logs
import settings

DATABASE_PATH = settings.DB_PATH
USERS_PATH = settings.USER_CSV_PATH
TRANSACTIONS_PATH = settings.TRANSACTIONS_CSV_PATH


def create_db_schemas(db_path):
    """

    :param db_path:
    :return:
    """

    if not db_path:
        logs.log_error('Database path not found.')

    try:
        conn = sqlite3.connect(db_path)

        cursor = conn.cursor()

        # Create users table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                signup_date TEXT,
                country TEXT
            );
        ''')
        logs.log_event(f'Task 1-1a Completed. User Table Created Successfully.')

        # Create transactions table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id INTEGER PRIMARY KEY,
                user_id INTEGER,
                transaction_date TEXT,
                amount REAL,
                transaction_type TEXT,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            );
        ''')
        logs.log_event(f'Task 1-1b Completed. Transactions Table Created Successfully.')

        conn.commit()
        conn.close()
        print("Task 1-1 Completed. Database schema created successfully.")
        logs.log_event("Task 1-1 Completed. Database schema created successfully.")
    except Exception as e:
        logs.log_error(f'Error Connecting to SQLite Database. Error code: {e}')


def load_users_to_db(db_path, csv_path):
    """
    Loads data from the users CSV into the database,
    skipping duplicates based on user_id.

    :param db_path:
    :param csv_path:
    :return:
    """

    if not db_path:
        logs.log_error(f'Database path not found.')

    if not csv_path:
        logs.log_error(f'User CSV path not found.')

    try:
        data = pd.read_csv(csv_path)
        conn = sqlite3.connect(db_path)

        for _, row in data.iterrows():
            # Insert or ignore duplicates in the users table
            query = '''
                INSERT OR IGNORE INTO users (user_id, signup_date, country)
                VALUES (?, ?, ?);
            '''
            conn.execute(query, (row['user_id'], row['signup_date'], row['country']))

        conn.commit()
        conn.close()
        print("Task 1-2a Completed. Users data ingested successfully.")
        logs.log_event("Task 1-2a Completed. Users data ingested successfully.")
    except Exception as e:
        logs.log_error(f'User data could not be ingested into SQLite Database. Error: {e}')

def load_transactions_to_db(db_path, csv_path):
    """
    Loads data from the transactions CSV into the database,
    updating existing records if necessary.

    :param db_path:
    :param csv_path:
    :return:
    """
    if not db_path:
        logs.log_error(f'Database path not found.')
    if not csv_path:
        logs.log_error(f'Transactions CSV path not found.')

    try:
        data = pd.read_csv(csv_path)
        conn = sqlite3.connect(db_path)

        for _, row in data.iterrows():
            # Insert or replace to handle duplicate transaction_id
            query = '''
                INSERT OR REPLACE INTO transactions (
                    transaction_id, user_id, transaction_date, amount, transaction_type
                )
                VALUES (?, ?, ?, ?, ?);
            '''
            conn.execute(query, (
                row['transaction_id'], row['user_id'], row['transaction_date'],
                row['amount'], row['transaction_type']
            ))

        conn.commit()
        conn.close()
        print("Task 1-2b Completed. Transactions data ingested successfully.")
        logs.log_event("Task 1-2b Completed. Transactions data ingested successfully.")
    except Exception as e:
        logs.log_error(f'Transaction data could not be ingested into SQLite Database. Error: {e}')


def delete_table(db_path, table_name):
    """
    Deletes a table from the SQLite database. Utility function for manual testing.

    :param db_path: Path to the SQLite database file
    :param table_name: Name of the table to be deleted
    """
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        query = f"DROP TABLE IF EXISTS {table_name};"

        cursor.execute(query)
        conn.commit()
        conn.close()

        print(f"Table '{table_name}' has been deleted successfully.")
        logs.log_event(f"Table '{table_name}' has been deleted successfully.")

    except sqlite3.Error as e:
        print(f"An error occurred while deleting the table: {e}")
        logs.log_error(f"Error occurred while deleting {table_name} table.")

def data_import_executive():
    """
    This function executes the steps to create the database schema and import the raw data from the CSV files.
    :return: None
    """

    if settings.DELETE_USER_TABLE:
        delete_table(DATABASE_PATH, "users")
    if settings.DELETE_TRANSACTION_TABLE:
        delete_table(DATABASE_PATH, "transactions")

    create_db_schemas(DATABASE_PATH)
    load_users_to_db(DATABASE_PATH, USERS_PATH)
    load_transactions_to_db(DATABASE_PATH, TRANSACTIONS_PATH)

    logs.log_event('Task 1 Completed Successfully. All data has been imported into SQLite Database.')

    print(f'Task 1 Completed. All data has been imported into SQLite Database.')
    print(f'-' * 30)