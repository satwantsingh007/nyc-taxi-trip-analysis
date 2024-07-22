# NYC Taxi Trip Analysis

## Project Overview
This project involves building an ETL pipeline to analyze NYC taxi trip data from 2019. The pipeline consists of data extraction, processing, loading, and visualization steps.

## Repository Structure

nyc-taxi-trip-analysis/
│
├── ETL_project/
│ ├── src/
│ │ ├── data_extraction.py
│ │ ├── data_processing.py
│ │ ├── data_loading.py
│ │ ├── data_visualization.py
│ │ ├── run_queries.py
│ ├── data/
│ │ ├── raw/
│ │ ├── processed/
│ ├── database/
│ │ ├── taxi_trips.db
│ ├── log/
│ ├── queries.sql
│ └── README.md


## How to Run the Project
1. Clone the repository:
   ```bash
   git clone https://github.com/satwantsingh007/nyc-taxi-trip-analysis.git

2. Navigate to the project directory:
   cd nyc-taxi-trip-analysis/ETL_project/src
3. Run the scripts in the following order:
  •  ‘data_extraction.py’: Downloads the parquet files.
  •  ‘data_processing.py’: Processes the downloaded files.
  •  ‘data_loading.py’: Loads the processed data into the SQLite database.
  •  ‘run_queries.py’: Executes the queries from ‘queries.sql’.
  •  ‘data_visualization.py’: Generates the data visualizations.

•  The project uses SQLite for the database and pandas for data processing.
•  Logging is implemented in each script to track the progress and errors.



