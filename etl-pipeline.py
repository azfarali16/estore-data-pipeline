from scripts.extraction.extract import extract_data
from scripts.transformation.transform import transform_data
from scripts.loading.load import load_csv_to_mysql
from scripts.metadata import *
import os

src_db_config = {
    'DB_HOST': 'Host',
    'DB_NAME': 'Name',
    'DB_USER': 'user',
    'DB_PASSWORD': 'password',
    'DB_PORT': 0000
}


db_config = {
    'DB_HOST': 'host',
    'DB_NAME': 'name',
    'DB_USER': 'user',
    'DB_PASSWORD': 'pass',
    'DB_PORT': 0000
}


PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))

METADATA_FILE_PATH = os.path.join(PROJECT_DIR, 'metadata', 'metadata.json')
RAW_DATA_PATH = os.path.join(PROJECT_DIR, 'data', 'raw')
TRANSFORMED_DATA_PATH = os.path.join(PROJECT_DIR, 'data', 'transformed')




def run_etl_pipeline():
    """
    Main function to run the ETL pipeline.
    This will:
    1. Extract data
    2. Transform data
    3. Load data
    """

    metadata = read_metadata(METADATA_FILE_PATH)
    last_surrogates_keys = metadata['surrogate_keys']


    print("Starting data extraction...")
    extract_data(src_db_config,RAW_DATA_PATH)
    
    print("Starting data transformation...")

    last_surrogates_keys = transform_data(RAW_DATA_PATH, TRANSFORMED_DATA_PATH, last_surrogates_keys)

    
    print("Loading data into MySQL database...")
    load_csv_to_mysql(db_config, TRANSFORMED_DATA_PATH)

    update_metadata(last_surrogates_keys,METADATA_FILE_PATH)

    print("ETL Pipeline Execution Completed.")


if __name__ == '__main__':
    run_etl_pipeline()