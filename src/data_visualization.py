import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import logging
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
log_rel_path = '../log'
LOG_DIR = os.path.join(script_dir, log_rel_path)
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
log_file = os.path.join(LOG_DIR, 'data_visualization.log')
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

def main():
    conn = None
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect('C:/Users/satwa/OneDrive/Desktop/D2k_project/ETL_project/database/taxi_trips.db')
        logging.info('Successfully connected to the SQLite database.')
        try:
            # Query for daily trips in 2019 and 2020
            daily_trips_query = """
            SELECT date, SUM(total_trips) AS total_trips
            FROM Taxi_trip
            WHERE strftime('%Y', date) IN ('2019', '2020')
            GROUP BY date
            ORDER BY date;
            """
            daily_trips_df = pd.read_sql(daily_trips_query, conn)
            daily_trips_df['date'] = pd.to_datetime(daily_trips_df['date'])
            logging.info('Daily trips data queried successfully.')
            # Query for monthly trips in 2019 and 2020
            monthly_trips_query = """
            SELECT strftime('%Y-%m-01', date) AS month, SUM(total_trips) AS total_trips
            FROM Taxi_trip
            WHERE strftime('%Y', date) IN ('2019', '2020')
            GROUP BY month
            ORDER BY month;
            """
            monthly_trips_df = pd.read_sql(monthly_trips_query, conn)
            monthly_trips_df['month'] = pd.to_datetime(monthly_trips_df['month'])
            logging.info('Monthly trips data queried successfully.')
        except Exception as e:
            logging.error(f'Error querying data: {e}')
            return
        try:
            # Plot daily trips over time
            plt.figure(figsize=(12, 6))
            plt.plot(daily_trips_df['date'], daily_trips_df['total_trips'], marker='o')
            plt.xlabel('Date')
            plt.ylabel('Total Trips')
            plt.title('Daily Trends in Taxi Usage Over 2019 and 2020')
            plt.xticks(rotation=45)
            plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=1))
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            plt.tight_layout()
            plt.show()
            logging.info('Daily trends plot generated successfully.')
            # Plot monthly trips over time
            plt.figure(figsize=(12, 6))
            plt.plot(monthly_trips_df['month'], monthly_trips_df['total_trips'], marker='o')
            plt.xlabel('Month')
            plt.ylabel('Total Trips')
            plt.title('Monthly Trends in Taxi Usage Over 2019 and 2020')
            plt.xticks(rotation=45)
            plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=1))
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            plt.tight_layout()
            plt.show()
            logging.info('Monthly trends plot generated successfully.')
        except Exception as e:
            logging.error(f'Error generating plots: {e}')
    finally:
        if conn:
            conn.close()
            logging.info('Database connection closed.')

if __name__ == '__main__':
    main()
