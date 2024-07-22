import pandas as pd
import logging
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
log_dir = os.path.join(script_dir, '../log')
log_file = os.path.join(log_dir, 'data_processing.log')

if not os.path.exists(log_dir):
    os.makedirs(log_dir)
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# The required columns for the schema
required_columns = ['pickup_datetime', 'dropoff_datetime', 'trip_miles', 'base_passenger_fare']

def process_file(file_path, processed_dir):
    logging.info(f'Starting to process file: {file_path}')
    try:
        df = pd.read_parquet(file_path)
        # Log the initial schema of the file
        logging.info(f'Schema of {file_path}:\n{df.dtypes}')
        # Rename columns to standard names
        renamed_columns = {
            'dispatching_base_num': 'dispatching_base_num',
            'pickup_datetime': 'pickup_datetime',
            'dropOff_datetime': 'dropoff_datetime',
            'PUlocationID': 'pulocationid',
            'DOlocationID': 'dolocationid',
            'SR_Flag': 'sr_flag',
            'Affiliated_base_number': 'affiliated_base_number'
        }
        df.rename(columns=renamed_columns, inplace=True)
        logging.info(f'Renamed columns: {renamed_columns}')
        # Check for missing required columns
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logging.warning(f'Required columns missing in {file_path}: {missing_columns}')
            return
        # Filter out the rows with any missing values in the required columns
        df = df.dropna(subset=required_columns)
        # Calculate trip duration in minutes
        df['trip_duration'] = (df['dropoff_datetime'] - df['pickup_datetime']).dt.total_seconds() / 60
        # Calculate average speed in miles per hour
        df['avg_speed_mph'] = df['trip_miles'] / (df['trip_duration'] / 60)
        # Aggregate data to calculate total trips and average fare per day
        df['pickup_date'] = df['pickup_datetime'].dt.date
        aggregated_data = df.groupby('pickup_date').agg(
            total_trips=pd.NamedAgg(column='pickup_datetime', aggfunc='count'),
            avg_fare=pd.NamedAgg(column='base_passenger_fare', aggfunc='mean')
        ).reset_index()

        # Save the processed file to the processed directory
        processed_file_path = os.path.join(processed_dir, os.path.basename(file_path))
        df.to_parquet(processed_file_path)
        logging.info(f'Processed data saved to {processed_file_path}')
    except Exception as e:
        logging.error(f'Error processing file {file_path}: {e}')

def main():
    raw_data_dir = os.path.join(script_dir, '../data/raw/')
    processed_data_dir = os.path.join(script_dir, '../data/processed/')
    if not os.path.exists(processed_data_dir):
        os.makedirs(processed_data_dir)
    for file_name in os.listdir(raw_data_dir):
        if file_name.endswith('.parquet'):
            raw_file_path = os.path.join(raw_data_dir, file_name)
            processed_file_path = os.path.join(processed_data_dir, file_name)
            # Check if the file has already been processed
            if os.path.exists(processed_file_path):
                logging.info(f'File already processed: {processed_file_path}')
                continue
            # Process the file
            process_file(raw_file_path, processed_data_dir)

if __name__ == '__main__':
    main()
