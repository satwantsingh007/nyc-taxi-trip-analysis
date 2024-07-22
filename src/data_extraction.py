import os
import time
import requests
from bs4 import BeautifulSoup
import logging

# Define constants
BASE_URL = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
YEAR = 2019
script_dir = os.path.dirname(os.path.abspath(__file__))
raw_data_rel_path = '../data/raw'
RAW_DATA_DIR = os.path.join(script_dir, raw_data_rel_path)
log_dir = os.path.join(script_dir, '../log')
log_file = os.path.join(log_dir, 'data_extraction.log')

# Setup the logging
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

def download_file(url, filename):
    try:
        if os.path.exists(filename):
            logging.info(f"File {filename} already exists, skipping download.")
            return True
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logging.info(f"Successfully downloaded {filename}")
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to download {filename}: {e}")
        return False

def main():
    if not os.path.exists(RAW_DATA_DIR):
        os.makedirs(RAW_DATA_DIR)
    try:
        response = requests.get(BASE_URL)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to retrieve base URL {BASE_URL}: {e}")
        return
    soup = BeautifulSoup(response.text, 'html.parser')
    links = soup.find_all('a', href=True)

    for link in links:
        href = link['href']
        if f"{YEAR}" in href and href.endswith('.parquet'):
            filename = os.path.join(RAW_DATA_DIR, href.split('/')[-1])
            retries = 5
            while retries > 0:
                if download_file(href, filename):
                    break
                retries -= 1
                logging.warning(f"Retrying download for {filename}, {retries} attempts left.")
                time.sleep(5)


if __name__ == "__main__":
    main()
