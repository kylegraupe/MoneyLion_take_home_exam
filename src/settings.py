"""
This file serves as a central location for global variables and paths.
"""

# Global Paths
DB_PATH = "transactions_data.db"
USER_CSV_PATH = "raw_data/users.csv"
TRANSACTIONS_CSV_PATH = "raw_data/transactions.csv"

# Usage Options
DISPLAY_DATA_INGESTION_TO_CONSOLE = False
DISPLAY_ETL_PROCESSES_TO_CONSOLE = False

# Use delete table functionality to manually testing application.
DELETE_USER_TABLE = True
DELETE_TRANSACTION_TABLE = True