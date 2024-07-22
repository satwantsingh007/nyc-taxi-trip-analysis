import sqlite3
import pandas as pd
import os
import logging

script_dir = os.path.dirname(os.path.abspath(__file__))
log_rel_path = '../log'
LOG_DIR = os.path.join(script_dir, log_rel_path)
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
log_file = os.path.join(LOG_DIR, 'run_queries.log')
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
# Define the path to the SQL script
sql_file_path = os.path.join(script_dir, 'queries.sql')

def execute_queries():
    conn = None
    try:
        database_dir = os.path.dirname(os.path.abspath(__file__))
        db_rel_path = '../database'
        DATABASE_DIR = os.path.join(database_dir, db_rel_path, 'taxi_trips.db')
        conn = sqlite3.connect(DATABASE_DIR)
        cursor = conn.cursor()
        logging.info('Successfully connected to the SQLite database.')
        # Read and execute the SQL script
        with open(sql_file_path, 'r') as file:
            sql_script = file.read()
        # Split the script into individual queries
        queries = sql_script.split(';')
        for query in queries:
            query = query.strip()
            if query:
                logging.info(f'Executing query: {query}')
                result_df = pd.read_sql_query(query, conn)
                logging.info(f'Query result:\n{result_df}')
    except Exception as e:
        logging.error(f'Error executing queries: {e}')

    finally:
        if conn:
            conn.close()
            logging.info('Database connection closed.')


if __name__ == '__main__':
    execute_queries()
