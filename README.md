# MoneyLion_take_home_exam
Machine Learning Platform Engineer Take Home Exam

## Features and Tasks

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
- utility_library.py: Contains reusable code such as query functionality to be used throughout the application.

### Setup Instructions

### Prerequisites
- Python 3.8 or higher
- Git (for cloning repository)
- pip (package manager for installing dependencies)

### Installation and Setup Steps
1. Clone Repository
   - Using Command Line: git clone https://github.com/kylegraupe/MoneyLion_take_home_exam.git
   - Or use GitHub GUI/Download as Zip
2. Set up Virtual Environment (Optional, but recommended)
   - Using a Python IDE such as PyCharm, this step is done automatically.
3. Configure Python Interpreter
   - Point the interpreter at main.py. This is the application endpoint for execution.
4. Install dependencies
   - A requirements.txt file is included in the repository.
   - Run: pip install -r requirements.txt to install dependencies. 

### Usage Instructions
1. PyCharm or other Python IDE is recommended. 
2. Ensure Python Interpreter is set up correctly.
3. Navigate to settings.py. Ensure these variables are correct before executing.
   - I added functionality to print the Pandas DataFrames for ETL processes directly to the console to visualize each step. These can be turned on or off by setting these variables to True/False.
   - The application executes with or without existing user and transaction tables, but if you would like to delete either table for manual testing, change the variables in the settings.py file to do so.
4. Execute the application by running the main.py file. 
5. Run the Unit Tests by executing the tests.py file.
6. Navigate to http://your_ip_address:5000/ to validate API endpoints. 
   - Test the API endpoints by navigating to the associated URLs as specified at the top of the flask_api.py file in the docstring. I left some examples for the different endpoints at the top of the script. Simple copy those URLs into a browser while the backend server is running to view the application endpoints. Make sure to update the IP address for your machine.
7. Navigate to the 'logs' folder during or after execution to view the logs from the run.

- If you have any questions with setup or installation, do not hesitate to reach out via email.

# Notes
- Unit tests and monitoring were implemented into the solution as examples of possible testing and monitoring techniques. In real-world applications, we can test and monitor in much further depth. However, for the scope of this assignment the tests and monitoring endpoints should be more than sufficient.

# Future Tasks
- Containerization: In future iterations, it would be possible to containerize this application through the use of common methods like Docker Images. This would be done by:
  1. Creating a DockerFile and a .dockerignore file
  2. Building a docker image
  3. Running the container
  4. Pushing to Docker Hub
- Simplification and Optimization
  - Certain components of this code could be further optimized, such as the clean_user_data() and clean_transaction_data() functions. However, for the scope of this assignment, the current approach works well.