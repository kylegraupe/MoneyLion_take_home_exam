"""
Task 5:

- Unit Test for individual functionality testing.
    - User Data Cleaning Tests: Tests clean_users_data() function with different forms of test data
    - Transaction Data Cleaning Tests: Tests clean_transaction_data() function with different forms of test data
    - ETL Function Tests: Tests form and quality of results of various ETL processes.

Note: Unit Tests not logged.
"""

import unittest
from unittest.mock import patch, MagicMock
import pandas as pd

import import_raw_to_db
import etl


class TestDataCleaning(unittest.TestCase):
    """
    Class to test data cleaning methods using example cases.
    """

    def test_clean_users_data_valid(self):
        """
        Tests function "clean_users_data()" to ensure viability with valid data.
        :return: None
        """
        data_valid = {
            'user_id': [1, 2, 3, 4],
            'signup_date': ['2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01'],
            'country': ['USA', 'Canada', 'UK', 'Germany']
        }
        df_valid = pd.DataFrame(data_valid)
        cleaned_df = import_raw_to_db.clean_users_data(df_valid)
        self.assertEqual(cleaned_df.shape, (4, 3), f"Expected 4 rows, got {cleaned_df.shape[0]}")

    def test_clean_users_data_missing_user_id(self):
        """
        Tests function "clean_users_data()" to ensure viability with missing User ID.

        :return: None
        """
        data_missing_user_id = {
            'user_id': [1, 2, None, 4],
            'signup_date': ['2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01'],
            'country': ['USA', 'Canada', 'UK', 'Germany']
        }
        df_missing_user_id = pd.DataFrame(data_missing_user_id)
        cleaned_df = import_raw_to_db.clean_users_data(df_missing_user_id)
        self.assertEqual(cleaned_df.shape, (3, 3), f"Expected 3 rows, got {cleaned_df.shape[0]}")

    def test_clean_users_data_invalid_date(self):
        """
        Tests function "clean_users_data()" to ensure viability with invalid dates.

        :return: None
        """
        data_invalid_date = {
            'user_id': [1, 2, 3],
            'signup_date': ['2024-01-01', 'invalid_date', '2024-03-01'],
            'country': ['USA', 'Canada', 'UK']
        }
        df_invalid_date = pd.DataFrame(data_invalid_date)
        cleaned_df = import_raw_to_db.clean_users_data(df_invalid_date)
        self.assertEqual(cleaned_df.shape, (2, 3), f"Expected 2 rows, got {cleaned_df.shape[0]}")

    def test_clean_transactions_data_valid(self):
        """
        Tests function "clean_transaction_data()" to ensure viability with valid data.

        :return: None
        """
        data_valid = {
            'transaction_id': [1, 2, 3, 4],
            'user_id': [101, 102, 103, 104],
            'transaction_date': ['2024-11-01', '2024-11-02', '2024-11-03', '2024-11-04'],
            'amount': [100.0, 50.0, 200.0, 150.0],
            'transaction_type': ['deposit', 'withdrawal', 'purchase', 'deposit']
        }
        df_valid = pd.DataFrame(data_valid)
        cleaned_df = import_raw_to_db.clean_transactions_data(df_valid)
        self.assertEqual(cleaned_df.shape, (4, 5), f"Expected 4 rows, got {cleaned_df.shape[0]}")

    def test_clean_transactions_data_invalid_type(self):
        """
        Tests function "clean_transaction_data()" to ensure viability with invalid transaction type data.

        :return:
        """
        data_invalid_type = {
            'transaction_id': [1, 2, 3],
            'user_id': [101, 102, 103],
            'transaction_date': ['2024-11-01', '2024-11-02', '2024-11-03'],
            'amount': [100.0, 50.0, 200.0],
            'transaction_type': ['deposit', 'invalid_type', 'purchase']
        }
        df_invalid_type = pd.DataFrame(data_invalid_type)
        cleaned_df = import_raw_to_db.clean_transactions_data(df_invalid_type)
        self.assertEqual(cleaned_df.shape, (2, 5), f"Expected 2 rows, got {cleaned_df.shape[0]}")

    def test_clean_transactions_data_invalid_amount(self):
        """
        Tests function "clean_transaction_data()" to ensure viability with invalid transaction amount. In this case, we
        are assuming that all transaction amounts must be positive given they have an associated type to delineate positive
        versus negative values.

        :return:
        """
        data_invalid_amount = {
            'transaction_id': [1, 2, 3],
            'user_id': [101, 102, 103],
            'transaction_date': ['2024-11-01', '2024-11-02', '2024-11-03'],
            'amount': [100.0, -50.0, 200.0],
            'transaction_type': ['deposit', 'withdrawal', 'purchase']
        }
        df_invalid_amount = pd.DataFrame(data_invalid_amount)
        cleaned_df = import_raw_to_db.clean_transactions_data(df_invalid_amount)
        self.assertEqual(cleaned_df.shape, (2, 5), f"Expected 2 rows, got {cleaned_df.shape[0]}")


class TestETLFunctions(unittest.TestCase):

    @patch('utility_library.execute_custom_query')
    def test_calculate_total_transaction_amount_per_user(self, mock_query):
        """
        Tests the total transaction calculation as defined by total transaction amount = sum(deposits, withdrawals, purchases)
        :param mock_query:
        :return:
        """
        # Prepare mock response
        mock_df = pd.DataFrame({
            'user_id': [1, 1],
            'total_transaction_amount': [1000, 2000],
            'total_deposit': [500, 1000],
            'total_withdrawal': [200, 300],
            'total_purchase': [300, 700]
        })
        mock_query.return_value = mock_df

        # Call function
        result = etl.calculate_total_transaction_amount_per_user()

        # Assertions
        mock_query.assert_called_once()
        self.assertEqual(result.shape, (2, 5))  # Verify DataFrame shape
        self.assertEqual(result['user_id'][0], 1)  # Verify values in DataFrame

        # Verify total transaction amount is the sum of all deposits, withdrawals, and purchases
        self.assertEqual(result['total_transaction_amount'].sum(),
                         sum([result['total_deposit'].sum(),
                              result['total_withdrawal'].sum(),
                              result['total_purchase'].sum()]))

    @patch('utility_library.execute_custom_query')
    def test_identify_top_ten_users_by_transaction_volume(self, mock_query):
        """
        Ensures the shape is correct when calculating top users by transaction volume.
        :param mock_query:
        :return:
        """
        # Prepare mock response
        mock_df = pd.DataFrame({
            'user_id': [1, 2],
            'transaction_volume': [100, 200]
        })
        mock_query.return_value = mock_df

        # Call function
        result = etl.identify_top_ten_users_by_transaction_volume()

        # Assertions
        mock_query.assert_called_once()
        self.assertEqual(result.shape, (2, 2))  # Verify DataFrame shape
        self.assertEqual(result['transaction_volume'][0], 100)

    @patch('utility_library.execute_custom_query')
    def test_aggregate_daily_transactions(self, mock_query):
        # Prepare mock response
        mock_df = pd.DataFrame({
            'transaction_date': ['2024-01-01', '2024-01-02'],
            'transaction_type': ['deposit', 'withdrawal'],
            'daily_total': [100, 200]
        })
        mock_query.return_value = mock_df

        # Call function
        result = etl.aggregate_daily_transactions()

        # Assertions
        mock_query.assert_called_once()
        self.assertEqual(result.shape, (2, 3))  # Verify DataFrame shape
        self.assertEqual(result['transaction_date'][0], '2024-01-01')


    @patch('sqlite3.connect')
    @patch('utility_library.execute_custom_query')
    def test_upsert_transaction_summary_to_users(self, mock_query, mock_connect):
        """
        Tests to ensure upsert logic accurately updates and inserts data into database using mock functionality.

        :param mock_query:
        :param mock_connect:
        :return:
        """
        # Mock DataFrame returned by calculate_total_transaction_amount_per_user
        mock_df = pd.DataFrame({
            'user_id': [1],
            'total_transaction_amount': [1000],
            'total_deposit': [500],
            'total_withdrawal': [200],
            'total_purchase': [300]
        })
        mock_query.return_value = mock_df

        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Call function
        etl.upsert_transaction_summary_to_users()

        # Assertions
        mock_cursor.execute.assert_called_with("""
        UPDATE users 
        SET 
            total_transaction_amount = ?,
            total_deposit = ?,
            total_withdrawal = ?,
            total_purchase = ?
        WHERE user_id = ?;
        """, (1000, 500, 200, 300, 1))
        mock_conn.commit.assert_called_once()


if __name__ == '__main__':
    unittest.main()
