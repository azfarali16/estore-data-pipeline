import pandas as pd
import os

# Import your helper functions and transformations
from .data_transformation_helpers import *
from .dim_fact_creation import *

def load_csv_files(csv_dir):
    tables = {}
    for filename in os.listdir(csv_dir):
        if filename.endswith('.csv'):
            table_name = filename.replace('.csv', '')
            file_path = os.path.join(csv_dir, filename)
            tables[table_name] = pd.read_csv(file_path)
    return tables

def save_transformed_data(tables_dict, save_path):
    os.makedirs(save_path, exist_ok=True)
    for table_name, table_df in tables_dict.items():
        table_filename = f"{table_name}.csv"
        table_filepath = os.path.join(save_path, table_filename)
        table_df.to_csv(table_filepath, index=False)
        print(f"Table '{table_name}' saved at {table_filepath}")



def transform_data(csv_dir, output_dir, last_surrogates_keys):
    last_surrogates_keys = last_surrogates_keys

    tables = load_csv_files(csv_dir)
    transformed_tables = {}

    # Apply transformations
    transformed_tables['category'] = transform_category(tables['category'])
    transformed_tables['customer'] = transform_customer(tables['customer'])
    transformed_tables['department'] = transform_department(tables['department'])
    transformed_tables['employee'] = transform_employee(tables['employee'])
    transformed_tables['inventory'] = transform_inventory(tables['inventory'])
    transformed_tables['location'] = transform_location(tables['location'])
    transformed_tables['manufacturer'] = transform_manufacturer(tables['manufacturer'])
    transformed_tables['payment'] = transform_payment(tables['payment'])
    transformed_tables['product'] = transform_product(tables['product'])
    transformed_tables['purchaseorder'] = transform_purchaseorder(tables['purchaseorder'])
    transformed_tables['purchaseorderdetail'] = transform_purchaseorderdetail(tables['purchaseorderdetail'])
    transformed_tables['returndetail'] = transform_returndetail(tables['returndetail'])
    transformed_tables['returns'] = transform_returns(tables['returns'])
    transformed_tables['salesorder'] = transform_salesorder(tables['salesorder'])
    transformed_tables['salesorderdetail'] = transform_salesorderdetail(tables['salesorderdetail'])
    transformed_tables['shipment'] = transform_shipment(tables['shipment'])
    transformed_tables['shipmentdetail'] = transform_shipmentdetail(tables['shipmentdetail'])
    transformed_tables['supplier'] = transform_supplier(tables['supplier'])
    transformed_tables['warehouse'] = transform_warehouse(tables['warehouse'])

    # Create dimension tables
    product_dim, last_surrogates_keys = create_product_dim(transformed_tables, last_surrogates_keys)
    supplier_dim, last_surrogates_keys = create_supplier_dim(transformed_tables, last_surrogates_keys)
    customer_dim, last_surrogates_keys = create_customer_dim(transformed_tables, last_surrogates_keys)
    warehouse_dim, last_surrogates_keys = create_warehouse_dim(transformed_tables, last_surrogates_keys)
    time_dim, last_surrogates_keys = create_time_dim(transformed_tables, last_surrogates_keys)

    # Create fact tables
    sales_fct = create_sales_fact_table(transformed_tables, time_dim, customer_dim, product_dim, last_surrogates_keys)
    purchase_fct = create_purchase_fact_table(transformed_tables, time_dim, supplier_dim, product_dim)
    inventory_fct = create_inventory_fact_table(transformed_tables, product_dim, warehouse_dim)
    return_fct = create_return_fact_table(transformed_tables, customer_dim, product_dim, time_dim)

    # Combine all the tables to be saved
    tables_to_save = {
        'product_dim': product_dim,
        'supplier_dim': supplier_dim,
        'customer_dim': customer_dim,
        'warehouse_dim': warehouse_dim,
        'time_dim': time_dim,
        'sales_fct': sales_fct,
        'purchase_fct': purchase_fct,
        'inventory_fct': inventory_fct,
        'return_fct': return_fct
    }

    # Save the transformed data
    save_transformed_data(tables_to_save, output_dir)

    # Return the updated surrogate keys for the next iteration
    return last_surrogates_keys