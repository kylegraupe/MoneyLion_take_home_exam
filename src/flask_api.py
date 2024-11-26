"""
Task 3: API Development
Develop a RESTful API to expose the processed data.
1. Get User Transaction Summary: get_user_transaction_summary()
    - Copy into browser to test: http://127.0.0.1:5000/api/user_transaction_summary?user_id=101
2. Get Top Users: get_top_users()
    - Copy into browser to test: http://127.0.0.1:5000/api/top_users
3. Get Daily Transactions: get_daily_transactions()
    - Copy into browser to test: http://127.0.0.1:5000/api/daily_transactions

Rather than pass the data from the ETL step to the API via a Pandas DataFrame, the API queries the data directly from
the database. This is a better practice as it pulls from the ground truth and avoids RAM saturation.

Task 4a: Monitoring
1. Monitor application performance and health
    - Copy into browser to test: http://127.0.0.1:5000/api/health
    - Copy into browser to test: http://127.0.0.1:5000/api/log_monitor
"""

from flask import Flask, jsonify, request
import psutil

import logs
import monitoring
import settings
import utility_library

app = Flask(__name__)

DATABASE_PATH = settings.DB_PATH


# 1. Get User Transaction Summary
@app.route('/api/user_transaction_summary', methods=['GET'])
def get_user_transaction_summary():
    """
    Handles the `/api/user_transaction_summary` endpoint to retrieve a user's transaction summary.

    Request Parameters:
    - `user_id` (str): The ID of the user whose transaction summary is to be retrieved.
      This parameter must be provided as a query string (e.g., `/api/user_transaction_summary?user_id=101`).
    :return: JSON response containing the user's transaction summary or an error message.
    """
    user_id = request.args.get('user_id')

    if not user_id:
        logs.log_error(f'Bad API Call: User ID is Required. Error Status: 400')
        return jsonify({'error': 'user_id is required'}), 400

    query = """
    SELECT 
        u.user_id, 
        u.country,
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
    WHERE 
        u.user_id = ?
    GROUP BY 
        u.user_id, u.country
    """
    result = utility_library.query_db_for_api(query, (user_id,))

    if not result:
        logs.log_error(f'Bad API Call: User ID is not found. Error Status: 404')
        return jsonify({'error': 'User not found'}), 404

    # Return the result in a JSON response
    logs.log_event(f'User {user_id} Transaction Summary call completed successfully and delivered to Flask Server.')
    return jsonify([dict(row) for row in result])

# 2. Get Top Users by Transaction Volume (Number of Transactions)
@app.route('/api/top_users', methods=['GET'])
def get_top_users():
    """
    Handles the `/api/top_users` endpoint to retrieve the top 10 users based on transaction volume.

    :return: JSON response containing the top 10 users by transaction volume or an error message.
    """
    query = """
    SELECT 
        u.user_id, 
        u.country,
        COUNT(t.transaction_id) AS transaction_count
    FROM 
        users u
    JOIN 
        transactions t
    ON 
        u.user_id = t.user_id
    GROUP BY 
        u.user_id, u.country
    ORDER BY 
        transaction_count DESC
    LIMIT 10
    """
    result = utility_library.query_db_for_api(query)

    if not result:
        logs.log_error(f'Bad API Call: Top Users by Transaction Volume Not Found. Status error: 404')
        return jsonify({'error': 'No users found'}), 404

    logs.log_event(f'Top Users by Transaction Volume Found and Delivered to Flask Server.')
    return jsonify([dict(row) for row in result])

# 3. Get Daily Transaction Totals by Transaction Type
@app.route('/api/daily_transactions', methods=['GET'])
def get_daily_transactions():
    """
    Handles the `/api/daily_transactions` endpoint to retrieve the daily transaction information.

    :return: JSON response containing the daily transaction data or an error message.
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
        t.transaction_date ASC, t.transaction_type
    """
    result = utility_library.query_db_for_api(query)

    if not result:
        logs.log_error(f'Bad API Call: Daily Transactions Not Found. Status error: 404')
        return jsonify({'error': 'No transactions found'}), 404

    logs.log_event(f'Daily Transactions Found and Delivered to Flask Server.')
    return jsonify([dict(row) for row in result])

@app.route("/api/health", methods=["GET"])
def health_check():
    """
    Health check endpoint to monitor application health.
    :return: JSON containing system performance metrics like CPU, memory, and uptime.
    """
    health_status = {
        "cpu_usage_%": psutil.cpu_percent(interval=1),
        "num_cores": psutil.cpu_count(),
        "memory_usage_%": psutil.virtual_memory().percent,
        "disk_usage_%": psutil.disk_usage('/').percent,
        "user_info":psutil.users()
    }

    logs.log_event(f'Health Check Successful and Delivered to Flask Server.')
    return jsonify(health_status), 200

@app.route("/api/log_monitor", methods=["GET"])
def log_monitoring():
    """
    Log Monitoring endpoint to monitor application status via logs.
    :return: JSON containing the number of INFO, WARNING, ERROR, and CRITICAL logs in the current log file.
    """
    counts = monitoring.count_log_levels(f'logs/{logs.LOG_FILE}')

    log_status = {
        "info_count": counts['INFO'],
        "warning_count": counts['WARNING'],
        "error_count": counts['ERROR'],
        "critical_count":counts['CRITICAL']
    }

    if not counts:
        logs.log_error(f'Log Monitoring Unsuccessful. Check log file exists.')

    logs.log_event(f'Log Monitoring Successful and Delivered to Flask Server.')
    return jsonify(log_status), 200