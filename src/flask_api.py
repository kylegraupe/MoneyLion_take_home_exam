"""
Task 3: API Development
Develop a RESTful API to expose the processed data.
1. Get User Transaction Summary: get_user_transaction_summary()
    - Copy into browser to test: http://127.0.0.1:5000/api/user_transaction_summary?user_id=101
2. Get Top Users: get_top_users()
    - Copy into browser to test: http://127.0.0.1:5000/api/top_users
3. Get Daily Transactions: get_daily_transactions()
    - Copy into browser to test: http://127.0.0.1:5000/api/daily_transactions

Task 4a: Monitoring
1. Monitor application performance and health
    - Copy into browser to test: http://127.0.0.1:5000/api/health
    - Copy into browser to test: http://127.0.0.1:5000/api/log_monitor
"""

from flask import Flask, jsonify, request
import sqlite3
import psutil

import logs
import monitoring
import settings

app = Flask(__name__)

# Database path
DATABASE_PATH = settings.DB_PATH


def query_db(query, params=()):
    """
    Function used to query SQLite Database
    :param query:
    :param params:
    :return:
    """
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row  # To access columns by name
    cur = conn.cursor()
    cur.execute(query, params)
    result = cur.fetchall()
    conn.close()
    return result


# 1. Get User Transaction Summary
@app.route('/api/user_transaction_summary', methods=['GET'])
def get_user_transaction_summary():
    """

    :return:
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
    result = query_db(query, (user_id,))

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

    :return:
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
    result = query_db(query)

    if not result:
        logs.log_error(f'Bad API Call: Top Users by Transaction Volume Not Found. Status error: 404')
        return jsonify({'error': 'No users found'}), 404

    logs.log_event(f'Top Users by Transaction Volume Found and Delivered to Flask Server.')
    return jsonify([dict(row) for row in result])


# 3. Get Daily Transaction Totals by Transaction Type
@app.route('/api/daily_transactions', methods=['GET'])
def get_daily_transactions():
    """

    :return:
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
    result = query_db(query)

    if not result:
        logs.log_error(f'Bad API Call: Daily Transactions Not Found. Status error: 404')
        return jsonify({'error': 'No transactions found'}), 404

    logs.log_event(f'Daily Transactions Found and Delivered to Flask Server.')
    return jsonify([dict(row) for row in result])

@app.route("/api/health", methods=["GET"])
def health_check():
    """
    Health check endpoint to monitor application health.
    Returns system performance metrics like CPU, memory, and uptime.
    """
    health_status = {
        "status": "healthy",
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
    Log Monitoring check endpoint to monitor application status.
    Returns the number of INFO, WARNING, and ERROR logs in the current log file.
    """
    counts = monitoring.count_log_levels(f'logs/{logs.LOG_FILE}')

    log_status = {
        "info_count": counts['INFO'],
        "warning_count": counts['WARNING'],
        "error_count": counts['ERROR'],
        "critical_count":counts['CRITICAL']
    }

    logs.log_event(f'Log Monitoring Successful and Delivered to Flask Server.')
    return jsonify(log_status), 200