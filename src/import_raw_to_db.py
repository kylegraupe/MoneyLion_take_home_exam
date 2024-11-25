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
    print("Database schema created successfully.") #TODO: CHANGE TO LOGGING

def load_csv_to_db(db_path, csv_path, table_name):
    """

    :param db_path:
    :param csv_path:
    :param table_name:
    :return:
    """
    data = pd.read_csv(csv_path)
    conn = sqlite3.connect(db_path)
    data.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()

    print(f"Data ingested successfully into {table_name} table.") #TODO: CHANGE TO LOGGING


def data_import_executive():
    """

    :return:
    """

    create_db_schemas(DATABASE_PATH)

    load_csv_to_db(DATABASE_PATH, USERS_PATH, "users")
    load_csv_to_db(DATABASE_PATH, TRANSACTIONS_PATH, "transactions")