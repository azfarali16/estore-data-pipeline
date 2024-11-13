import json
from datetime import datetime

def read_metadata(METADATA_FILE_PATH):
    """Reads the metadata from the metadata.json file."""
    try:
        with open(METADATA_FILE_PATH, 'r') as f:
            metadata = json.load(f)
        return metadata
    except FileNotFoundError:
        print(f"{METADATA_FILE_PATH} not found, creating a new one.")
        return {
            "last_etl_run": None,
            "surrogate_keys": {
                "ProductKey": 0,
                "SupplierKey": 0,
                "CustomerKey": 0,
                "WarehouseKey": 0,
                "TimeKey": 0
            }
        }


def update_metadata(last_surrogates_keys,METADATA_FILE_PATH):
    """Update the metadata.json file with the latest ETL run details."""
    metadata = read_metadata(METADATA_FILE_PATH)
    
    # Update surrogate keys
    metadata["surrogate_keys"] = last_surrogates_keys
    
    # Set today's date as the last ETL run date
    metadata["last_etl_run"] = datetime.now().strftime('%Y-%m-%d')
    
    # Write the updated metadata back to the file
    with open(METADATA_FILE_PATH, 'w') as f:
        json.dump(metadata, f, indent=4)

    print(f"Metadata updated: {metadata['last_etl_run']} | Surrogate keys: {last_surrogates_keys}")
