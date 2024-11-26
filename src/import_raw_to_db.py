"""
TASK 1: Data Ingestion and Database Design
1. Design a relational database schema to store the data efficiently (Use OpenSource libraries
like SQLite).
2. Implement a script to ingest the data from the CSV files into your database.
"""

import sqlite3
import pandas as pd

DATABASE_PATH = "transactions_data.db"
USERS_PATH = "raw_data/users.csv"
TRANSACTIONS_PATH = "raw_data/transactions.csv"


def create_db_schemas(db_path):
    """

    :param db_path:
    :return:
    """

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

    conn.commit()
    conn.close()
    print("Database schema created successfully.")  #TODO: CHANGE TO LOGGING

def load_users_to_db(db_path, csv_path):
    """
    Loads data from the users CSV into the database,
    skipping duplicates based on user_id.

    :param db_path:
    :param csv_path:
    :return:
    """
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
    print("Users data ingested successfully.")  # TODO: CHANGE TO LOGGING

def load_transactions_to_db(db_path, csv_path):
    """
    Loads data from the transactions CSV into the database,
    updating existing records if necessary.

    :param db_path:
    :param csv_path:
    :return:
    """
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
    print("Transactions data ingested successfully.")  # TODO: CHANGE TO LOGGING

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

def delete_table(db_path, table_name):
    """
    Deletes a table from the SQLite database.

    :param db_path: Path to the SQLite database file
    :param table_name: Name of the table to be deleted
    """
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create the SQL query to drop the table
        query = f"DROP TABLE IF EXISTS {table_name};"

        # Execute the query
        cursor.execute(query)

        # Commit the changes
        conn.commit()

        # Close the connection
        conn.close()

        print(f"Table '{table_name}' has been deleted successfully.")

    except sqlite3.Error as e:
        print(f"An error occurred while deleting the table: {e}")

def data_import_executive():
    """

    :return:
    """

    create_db_schemas(DATABASE_PATH)
    load_users_to_db(DATABASE_PATH, USERS_PATH)
    load_transactions_to_db(DATABASE_PATH, TRANSACTIONS_PATH)

    # delete_table(DATABASE_PATH, "users")

    user_test_query = "SELECT * from users"
    # transaction_test_query = "SELECT * FROM transactions WHERE transaction_type = 'deposit';"

    print(execute_custom_query(user_test_query))
    # print(execute_custom_query(transaction_test_query))
