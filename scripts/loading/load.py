import pymysql
import pandas as pd
import os

def load_csv_to_mysql(db_config, csv_dir):
    """
    Load CSV files from a directory into a MySQL database.
    Each CSV file will be loaded into a table with the same name as the file (without the .csv extension).
    """
    conn = None

    try:
        # Establish the connection to MySQL using PyMySQL
        conn = pymysql.connect(
            host=db_config['DB_HOST'],
            database=db_config['DB_NAME'],
            user=db_config['DB_USER'],
            password=db_config['DB_PASSWORD'],
            port=db_config['DB_PORT']
        )

        if conn.open:
            print(f"Connected to MySQL database {db_config['DB_NAME']}")

            # Loop through each CSV file in the directory
            for file_name in os.listdir(csv_dir):
                if file_name.endswith('.csv'):
                    table_name = file_name.replace('.csv', '')  # Table name from file name
                    file_path = os.path.join(csv_dir, file_name)
                    df = pd.read_csv(file_path)

                    # Prepare columns for creating table
                    columns = ", ".join([f"`{col}` TEXT" for col in df.columns])

                    # Create a table if not exists
                    create_table_query = f"""
                    CREATE TABLE IF NOT EXISTS `{table_name}` (
                        {columns}
                    );
                    """
                    cursor = conn.cursor()
                    cursor.execute(create_table_query)
                    print(f"Table '{table_name}' created or already exists.")

                    # Insert data into the table
                    for _, row in df.iterrows():
                        insert_query = f"INSERT INTO `{table_name}` ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(row))})"
                        cursor.execute(insert_query, tuple(row))
                    conn.commit()
                    print(f"Loaded '{file_name}' into MySQL table '{table_name}'")

            cursor.close()
        else:
            print("Failed to connect to MySQL")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if conn:
            conn.close()
            print("Connection closed.")
