import sqlite3
import pandas as pd
from pathlib import Path
import logging
import os

# Configure logging
script_dir = os.path.dirname(os.path.abspath(__file__))
log_dir = os.path.join(script_dir, '../log')
log_file = os.path.join(log_dir, 'data_loading.log')

if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Define database file path
database_file_path = os.path.join(script_dir, '../database/taxi_trips.db')

# Establish SQLite database connection
try:
    conn = sqlite3.connect(database_file_path)
    cursor = conn.cursor()
    logging.info(f"Connected to SQLite database at {database_file_path}")
except sqlite3.Error as e:
    logging.error(f"Error connecting to SQLite database: {e}")
    raise

# Create table schema based on the provided columns in the Parquet files
create_table_query = """
CREATE TABLE IF NOT EXISTS Taxi_trip (
    date DATE,
    total_trips INTEGER,
    average_fare REAL
);
"""
try:
    cursor.execute(create_table_query)
    conn.commit()
    logging.info("Taxi_trip table created or already exists.")
except sqlite3.Error as e:
    logging.error(f"Error creating Taxi_trip table: {e}")
    conn.close()
    raise

# Load processed data files
processed_data_path = Path(os.path.join(script_dir, '../data/processed'))

def load_data_to_db(file_path):
    try:
        df = pd.read_parquet(file_path)
        df.to_sql('Taxi_trip', conn, if_exists='append', index=False)
        logging.info(f"Loaded {file_path} into the database.")
    except Exception as e:
        logging.error(f"Error loading {file_path} into the database: {e}")

# Iterate through processed files and load them into the database
for file_path in processed_data_path.glob('*.parquet'):
    load_data_to_db(file_path)

# Close the database connection
conn.close()
logging.info("Data loading complete and connection closed.")
