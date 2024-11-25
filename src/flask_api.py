from flask import Flask, jsonify, request
import sqlite3
import pandas as pd

app = Flask(__name__)

# Database path
DATABASE_PATH = "transactions_data.db"

