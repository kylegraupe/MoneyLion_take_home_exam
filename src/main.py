import sqlite3
import import_raw_to_db

# # Connect to the database
# db_path = "transactions_data.db"
# conn = sqlite3.connect(db_path)
# cursor = conn.cursor()

if __name__ == "__main__":
    print("Hello, World!")

    import_raw_to_db.data_import_executive()

    # cursor.execute("SELECT * FROM users;")
    # users_data = cursor.fetchall()
    # print("Users Table:")
    # for row in users_data:
    #     print(row)