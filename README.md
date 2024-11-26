# MoneyLion_take_home_exam
Machine Learning Platform Engineer Take Home Exam

## Features

- Task 1: Data Ingestion and Database Design
  - Script: import_raw_to_db.py
  - Overview: Imports CSV files into Pandas DataFrame, filters out bad data, and stores raw data in SQLite Database.
  - Assumptions:
    - User IDs and Transaction IDs are unique.
- Task 2: ETL Pipeline
  - Script: etl.py
  - Overview: Extracts raw data from database, transforms data using instructions, loads the data back into SQLite Database for use by Flask API.
  - Assumptions:
    - Total transaction amount per user is equal to the sum of all transactions for that user. Deposits, withdrawals, and purchases are all positive values. This instruction was ambiguous as this could mean many things.
    - Transaction volume is calculated by the number of transactions for a given user.
    - Daily transaction aggregates are split into deposits, withdrawals, and purchases. These instructions were ambiguous.
- Task 3: API Development
  - Script: flask_api.py
  - Overview: Provides endpoints via Flask API for user transaction summary, top ten users by transaction volume, and daily transactions. Also provides endpoints for monitoring.
  - Assumptions:
    - Transaction summary is defined as a user's transaction statistics.
- Task 4a: Monitoring
  - Scripts: flask_api.py, monitoring.py
  - Overview: Provides API endpoints for application monitoring.
    - Functions: 
      - health_check(): gives an endpoint for application health via resource usage.
      - log_monitoring(): gives an endpoint for application performance via log monitoring.
- Task 4b: Logging
  - Scripts: logs.py, etl.py, flask_api.py, import_raw_to_db.py, main.py
  - Overview: Key Events, Errors, and Warnings are logged and stored as a log file.
- Task 5: Testing
  - Scripts: test.py
  - Overview: Unit tests for individual component testing.

General:
- main.py: Execution endpoint. Configure Python Interpreter to this file.
- settings.py: Contains global variables and paths to be used throughout the application.
- utility_library: Contains reusable code such as query functionality to be used throughout the application.

### Setup Instructions

### Prerequisites

### Installation Steps

### Usage Instructions

### Testing

## Notes

## Assumptions

