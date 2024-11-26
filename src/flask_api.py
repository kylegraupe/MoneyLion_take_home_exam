"""
Task 3: API Development
Develop a RESTful API to expose the processed data.
1. Get User Transaction Summary: get_user_transaction_summary()
    - Copy into browser to test: http://127.0.0.1:5000/api/user_transaction_summary?user_id=101
2. Get Top Users: get_top_users()
    - Copy into browser to test: http://127.0.0.1:5000/api/top_users
3. Get Daily Transactions: get_daily_transactions()
    - Copy into browser to test: http://127.0.0.1:5000/api/daily_transactions
"""

from flask import Flask, jsonify, request
import sqlite3

app = Flask(__name__)

# Database path
DATABASE_PATH = "transactions_data.db"


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
        return jsonify({'error': 'User not found'}), 404

    # Return the result in a JSON response
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
        return jsonify({'error': 'No users found'}), 404

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
        return jsonify({'error': 'No transactions found'}), 404

    return jsonify([dict(row) for row in result])


