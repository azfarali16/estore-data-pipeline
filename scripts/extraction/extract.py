import psycopg2
import os
import pandas as pd

def connect_to_db(db_config):
    """Establish connection to the PostgreSQL database using a connection dictionary."""
    return psycopg2.connect(
        host=db_config['DB_HOST'],
        dbname=db_config['DB_NAME'],
        user=db_config['DB_USER'],
        password=db_config['DB_PASSWORD'],
        port=db_config['DB_PORT']
    )

def get_tables(cursor):
    """Fetch the list of all tables in the 'public' schema."""
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
    """)
    return [table[0] for table in cursor.fetchall()]

def fetch_and_save_table_to_csv(cursor, table_name, output_dir):
    """Fetch data from a table and save it as a CSV file."""
    cursor.execute(f"SELECT * FROM {table_name};")

    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    df = pd.DataFrame(rows, columns=columns)
    file_path = os.path.join(output_dir, f"{table_name}.csv")
    df.to_csv(file_path, index=False)

    print(f"Table '{table_name}' saved as CSV at {file_path}")

def extract_data(db_config, output_dir):
    """
    Extract data from the database and save it to CSV files.

    Parameters:
    - db_config (dict): A dictionary containing database connection parameters.
    - output_dir (str): The directory where the CSV files will be saved.
    """
    conn = connect_to_db(db_config)
    cursor = conn.cursor()

    try:
        tables = get_tables(cursor)
        for table in tables:
            fetch_and_save_table_to_csv(cursor, table, output_dir)
    
    finally:
        cursor.close()
        conn.close()
