"""
Execution endpoint.
"""

import sqlite3
import pandas as pd

import import_raw_to_db
import etl
import flask_api
import settings
import logs

DATABASE_PATH = settings.DB_PATH


if __name__ == "__main__":
    logs.log_event(f'GLOBAL SETTINGS: \n'
                   f'\tDatabase Path = {settings.DB_PATH}\n'
                   f'\tUser CSV Path = {settings.USER_CSV_PATH}\n'
                   f'\tTransactions CSV Path = {settings.TRANSACTIONS_CSV_PATH}\n'
                   f'\tDisplay ETL Processes = {settings.DISPLAY_ETL_PROCESSES_TO_CONSOLE}\n'
                   f'\tDisplay Data Ingestion Processes = {settings.DISPLAY_DATA_INGESTION_TO_CONSOLE}\n'
                   f'\tDelete User Table = {settings.DELETE_USER_TABLE}\n'
                   f'\tDelete Transactionn = {settings.DELETE_TRANSACTION_TABLE}\n')

    import_raw_to_db.data_import_executive()
    etl.etl_executive()
    flask_api.app.run(debug=True, use_reloader=False)
