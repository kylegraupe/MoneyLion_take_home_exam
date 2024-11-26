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
from datetime import datetime

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

def clean_users_data(df):
    """
    Cleans a pandas DataFrame containing user information by handling poor data quality.

    **Schema**:
    - `user_id`: Must be unique, non-null, and a positive integer.
    - `signup_date`: Must be a valid date in the format `YYYY-MM-DD`.
    - `country`: Must be non-null and contain valid country names.

    **Steps**:
    1. Remove rows with missing `user_id`, invalid `signup_date`, or missing `country`.
    2. Ensure `user_id` is unique and positive.
    3. Handle invalid or missing `signup_date` by filtering out rows with improperly formatted dates.
    4. Optional: Replace missing `country` entries with "Unknown" (or a default).

    :param df: Input pandas DataFrame.
    :return: Cleaned pandas DataFrame and a dictionary with the count of poor data instances.
    """
    # Validate and filter out rows with invalid `signup_date`
    def is_invalid_date(date_str):
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            return False
        except (ValueError, TypeError):
            return True

    def is_valid_date(date_str):
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            return True
        except (ValueError, TypeError):
            return False

    missing_user_id = df[df['user_id'].isna()]
    if len(missing_user_id) != 0:
        logs.log_warning(f'There are {len(missing_user_id)} instances of missing User IDs. These rows have been dropped')
    df = df[df['user_id'].notna()]

    invalid_user_id = df[df['user_id'].apply(lambda x: not isinstance(x, int) and x <= 0)]
    if len(invalid_user_id) != 0:
        logs.log_warning(f'There are {len(invalid_user_id)} instances of invalid User IDs. These rows have been dropped.')
    df = df[df['user_id'].apply(lambda x: isinstance(x, int) and x > 0)]

    invalid_signup_date = df[df['signup_date'].apply(is_invalid_date)]
    if len(invalid_signup_date) != 0:
        logs.log_warning(f'There are {invalid_signup_date.sum()} instances of invalid signup dates. These rows have been dropped.')
    df = df[df['signup_date'].apply(is_valid_date)]

    # Count rows with missing `country` values
    missing_country = df[df['country'].isnull()]
    if len(missing_country):
        logs.log_warning(f'There are {len(missing_country)} instances of invalid user country. These rows have been dropped.')
    df = df[df['country'].notna()]

    # Ensure `user_id` is unique
    duplicate_user_id = df['user_id'].duplicated(keep=False)
    if duplicate_user_id.sum() != 0:
        logs.log_warning(f'There are {duplicate_user_id.sum()} instances of duplicate User IDs. These rows have been dropped')
    df = df[~duplicate_user_id]
    df = df.reset_index(drop=True)

    dropped_row_num = sum([len(missing_user_id), len(invalid_user_id), len(invalid_signup_date), len(missing_country), duplicate_user_id.sum()])

    logs.log_event(f'User data cleaned. {dropped_row_num} rows have been dropped.')
    if settings.DISPLAY_DATA_INGESTION_TO_CONSOLE:
        print(f'\tUser Data Cleaned. {dropped_row_num} rows have been dropped.')
    return df

def clean_transactions_data(df):
    """
    Cleans a pandas DataFrame containing transaction information by handling poor data quality.

    1. Remove rows with missing `transaction_id`, invalid `user_id`, invalid `transaction_date`, invalid `amount`, or invalid `transaction_type`.
    2. Ensure `transaction_id` is unique and positive.
    3. Handle invalid or missing `transaction_date` by filtering out rows with improperly formatted dates.
    4. Handle `amount` being non-positive (set to NaN or handle as invalid).
    5. Handle `transaction_type` by ensuring it's one of the valid types.

    :param df: Input pandas DataFrame.
    :return: Cleaned pandas DataFrame.
    """
    poor_data_count = {
        'missing_transaction_id': 0,
        'invalid_transaction_id': 0,
        'missing_user_id': 0,
        'invalid_transaction_date': 0,
        'invalid_amount': 0,
        'invalid_transaction_type': 0,
        'duplicate_transaction_id': 0
    }

    # Validate and filter out rows with invalid `transaction_id`
    missing_transaction_id = df[df['transaction_id'].isna()]
    poor_data_count['missing_transaction_id'] = len(missing_transaction_id)
    df = df[df['transaction_id'].notna()]

    invalid_transaction_id = df[df['transaction_id'].apply(lambda x: not isinstance(x, int) or x <= 0)]
    poor_data_count['invalid_transaction_id'] = len(invalid_transaction_id)
    df = df[df['transaction_id'].apply(lambda x: isinstance(x, int) and x > 0)]

    # Validate and filter out rows with invalid `user_id`
    missing_user_id = df[df['user_id'].isna()]
    poor_data_count['missing_user_id'] = len(missing_user_id)
    df = df[df['user_id'].notna()]

    # Validate and filter out rows with invalid `transaction_date`
    def is_valid_date(date_str):
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            return True
        except (ValueError, TypeError):
            return False

    invalid_transaction_date = df[df['transaction_date'].apply(lambda x: not is_valid_date(x))]
    poor_data_count['invalid_transaction_date'] = len(invalid_transaction_date)
    df = df[df['transaction_date'].apply(is_valid_date)]

    # Validate and filter out rows with invalid `amount` (non-positive values)
    invalid_amount = df[df['amount'] <= 0]
    poor_data_count['invalid_amount'] = len(invalid_amount)
    df = df[df['amount'] > 0]

    # Validate and filter out rows with invalid `transaction_type`
    valid_transaction_types = ['deposit', 'withdrawal', 'purchase']
    invalid_transaction_type = df[~df['transaction_type'].isin(valid_transaction_types)]
    poor_data_count['invalid_transaction_type'] = len(invalid_transaction_type)
    df = df[df['transaction_type'].isin(valid_transaction_types)]

    # Ensure `transaction_id` is unique
    duplicate_transaction_id = df['transaction_id'].duplicated(keep=False)
    poor_data_count['duplicate_transaction_id'] = duplicate_transaction_id.sum()
    df = df[~duplicate_transaction_id]

    # Reset the index for a clean output DataFrame
    df = df.reset_index(drop=True)

    dropped_row_num = sum(poor_data_count.values())

    logs.log_event(f'Transaction data cleaned. {dropped_row_num} rows have been dropped.')
    if settings.DISPLAY_DATA_INGESTION_TO_CONSOLE:
        print(f'\tTransaction Data Cleaned. {dropped_row_num} rows have been dropped.')

    return df

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
        data = clean_users_data(data)
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
        data = clean_transactions_data(data)
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