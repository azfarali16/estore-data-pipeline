
import pandas as pd


def create_product_dim(transformed_tables, last_surrogates_keys):
    # Extract necessary dataframes
    product = transformed_tables['product']
    category = transformed_tables['category']
    manufacturer = transformed_tables['manufacturer']

    # Step 1: Merge product with category and manufacturer
    tempdf = pd.merge(product, category, on='categoryid', how='left', suffixes=('_product', '_category'))
    tempdf = pd.merge(tempdf, manufacturer, on='manufacturerid', how='left', suffixes=('', '_manufacturer'))

    # Step 2: Rename columns to match the product_dim schema
    rename_dict = {
        'productid': 'ProductID',
        'name_product': 'Name',                   # Product name
        'name_category': 'CategoryName',          # Category name
        'name': 'ManufacturerName',               # Manufacturer name
        'price': 'Price',                         # Product price
        'stocklevel': 'StockLevel',               # Product stock level
        'reorderlevel': 'ReorderLevel',           # If needed, not part of your schema, so can be omitted
        'discontinued': 'Discontinued',           # If needed, not part of your schema, so can be omitted
        'categoryid': 'CategoryID',               # Category ID
        'manufacturerid': 'ManufacturerID',       # Manufacturer ID
    }
    tempdf.rename(columns=rename_dict, inplace=True)

    # Step 3: Assign surrogate keys
    tempdf['ProductKey'] = range(last_surrogates_keys['ProductKey'] + 1, last_surrogates_keys['ProductKey'] + len(tempdf) + 1)
    last_surrogates_keys['ProductKey'] = tempdf['ProductKey'].max()

    # Optional: Handle missing values if necessary
    tempdf['StockLevel'] = tempdf['StockLevel'].fillna(0)

    # Step 4: Remove duplicates
    tempdf = tempdf.drop_duplicates()

    # Step 5: Reorder columns as per the schema
    reordered_columns = [
        'ProductKey','ProductID', 'Name', 'Discontinued' ,'CategoryID', 'CategoryName',
        'ManufacturerID', 'ManufacturerName', 'Price', 'StockLevel'
    ]
    product_dim = tempdf[reordered_columns]

    # Return the final product_dim dataframe
    return product_dim, last_surrogates_keys






def create_supplier_dim(transformed_tables, last_surrogates_keys):
    # Extract the supplier dataframe
    supplier = transformed_tables['supplier']

    # Select required columns
    supplier_dim = supplier[['supplierid', 'name', 'country', 'rating', 'contractstartdate', 'contractenddate']]

    # Step 1: Rename columns to match the supplier_dim schema
    supplier_dim = supplier_dim.copy()
    supplier_dim.rename(columns={
        'supplierid': 'SupplierID',
        'name': 'Name',
        'country': 'Country',
        'rating': 'Rating',
        'contractstartdate': 'ContractStartDate',
        'contractenddate': 'ContractEndDate'
    }, inplace=True)

    # Step 2: Handle missing values and data types
    supplier_dim['Rating'] = supplier_dim['Rating'].fillna('Unknown')  # Default rating if missing
    supplier_dim['ContractStartDate'] = pd.to_datetime(supplier_dim['ContractStartDate'], errors='coerce')
    supplier_dim['ContractEndDate'] = pd.to_datetime(supplier_dim['ContractEndDate'], errors='coerce')

    # Step 3: Remove duplicates
    supplier_dim = supplier_dim.drop_duplicates()

    # Step 4: Assign surrogate keys
    supplier_dim['SupplierKey'] = range(last_surrogates_keys['SupplierKey'] + 1, last_surrogates_keys['SupplierKey'] + len(supplier_dim) + 1)
    last_surrogates_keys['SupplierKey'] = supplier_dim['SupplierKey'].max()  # Update the last SupplierKey value

    # Step 5: Reorder columns to match the supplier_dim schema
    reorder = ['SupplierKey', 'SupplierID', 'Name', 'Country', 'Rating', 'ContractStartDate', 'ContractEndDate']
    supplier_dim = supplier_dim[reorder]

    # Return the final supplier_dim dataframe and updated surrogate keys
    return supplier_dim, last_surrogates_keys





def create_customer_dim(transformed_tables, last_surrogates_keys):
    # Extract the customer dataframe
    customer = transformed_tables['customer']

    # Select required columns
    customer_dim = customer[['customerid', 'name', 'address', 'preferredpaymentmethod', 'creditlimit']]

    # Step 1: Rename columns to match the customer_dim schema
    customer_dim = customer_dim.copy()
    customer_dim.rename(columns={
        'customerid': 'CustomerID',
        'name': 'Name',
        'address': 'Address',
        'preferredpaymentmethod': 'PreferredPaymentMethod',
        'creditlimit': 'CreditLimit'
    }, inplace=True)

    # Step 2: Handle missing values
    customer_dim['CreditLimit'] = customer_dim['CreditLimit'].fillna(0)  # Default to 0 if missing
    customer_dim['PreferredPaymentMethod'] = customer_dim['PreferredPaymentMethod'].fillna('Unknown')

    # Step 3: Remove duplicates
    customer_dim = customer_dim.drop_duplicates()

    # Step 4: Assign surrogate keys
    customer_dim['CustomerKey'] = range(
        last_surrogates_keys['CustomerKey'] + 1, 
        last_surrogates_keys['CustomerKey'] + len(customer_dim) + 1
    )
    last_surrogates_keys['CustomerKey'] = customer_dim['CustomerKey'].max()  # Update the last CustomerKey value

    # Step 5: Reorder columns to match the customer_dim schema
    reorder = ['CustomerKey', 'CustomerID', 'Name', 'Address', 'PreferredPaymentMethod', 'CreditLimit']
    customer_dim = customer_dim[reorder]

    # Return the final customer_dim dataframe and updated surrogate keys
    return customer_dim, last_surrogates_keys






def create_warehouse_dim(transformed_tables, last_surrogates_keys):
    # Extract the warehouse and location dataframes
    warehouse = transformed_tables['warehouse']
    location = transformed_tables['location']

    # Step 1: Merge warehouse with location data
    warehouse_with_location = pd.merge(warehouse, location, on='locationid', how='left', suffixes=('_warehouse', '_location'))

    # Step 2: Select relevant columns and rename them
    warehouse_dim = warehouse_with_location[['warehouseid', 'capacity', 'locationid', 'name', 'country', 'city']]

    # Step 3: Rename columns to match the warehouse_dim schema
    warehouse_dim = warehouse_dim.copy()
    warehouse_dim.rename(columns={
        'warehouseid': 'WarehouseID',
        'capacity': 'Capacity',
        'locationid': 'LocationID',
        'name': 'LocationName',
        'country': 'Country',
        'city': 'City'
    }, inplace=True)

    # Step 4: Handle missing values
    warehouse_dim['LocationName'] = warehouse_dim['LocationName'].fillna('Unknown')
    warehouse_dim['Country'] = warehouse_dim['Country'].fillna('Unknown')
    warehouse_dim['City'] = warehouse_dim['City'].fillna('Unknown')

    # Step 5: Remove duplicates
    warehouse_dim = warehouse_dim.drop_duplicates()

    # Step 6: Assign surrogate keys
    warehouse_dim['WarehouseKey'] = range(
        last_surrogates_keys['WarehouseKey'] + 1, 
        last_surrogates_keys['WarehouseKey'] + len(warehouse_dim) + 1
    )
    last_surrogates_keys['WarehouseKey'] = warehouse_dim['WarehouseKey'].max()  # Update the last WarehouseKey value

    # Step 7: Reorder columns to match the warehouse_dim schema
    reorder = ['WarehouseKey', 'WarehouseID', 'Capacity', 'LocationID', 'LocationName', 'Country', 'City']
    warehouse_dim = warehouse_dim[reorder]

    # Return the final warehouse_dim dataframe and updated surrogate keys
    return warehouse_dim, last_surrogates_keys






def create_time_dim(transformed_tables, last_surrogates_keys):
    # List of tables that contain date columns
    date_columns = {
        'employee': ['hiredate'],
        'inventory': ['lastreorderdate', 'expecteddeliverydate'],
        'purchaseorder': ['orderdate', 'expecteddeliverydate', 'actualdeliverydate'],
        'payment': ['paymentdate'],
        'returns': ['returndate'],
        'salesorder': ['orderdate', 'actualdeliverydate'],
        'shipment': ['shipmentdate', 'estimatedarrivaldate', 'actualarrivaldate'],
        'supplier': ['contractstartdate', 'contractenddate']
    }

    # Extract dates from all tables, replacing NA with '1900-01-01'
    dates = []

    for table_name, columns in date_columns.items():
        for column in columns:
            if column in transformed_tables[table_name].columns:
                # Fill NA values with '1900-01-01'
                filled_dates = pd.to_datetime(transformed_tables[table_name][column].fillna(pd.Timestamp('1900-01-01')), errors='coerce')
                dates.extend(filled_dates.dropna().tolist())

    # Convert list of dates to a pandas datetime series
    dates = pd.to_datetime(dates, errors='coerce').dropna().sort_values().unique()

    # Step 2: Start from the next available date after '1900-01-01'
    min_date = pd.Timestamp('1900-01-01')
    next_date = min(dates[dates > min_date]) if len(dates[dates > min_date]) > 0 else min_date

    # Step 3: Create the date range from the next available date to the latest date
    start_date = next_date
    end_date = dates.max()
    date_range = pd.date_range(start=start_date, end=end_date)

    # Step 4: Create the time dimension table
    time_dim = pd.DataFrame(date_range, columns=['Date'])

    # Extracting various time attributes
    time_dim['Year'] = time_dim['Date'].dt.year
    time_dim['Quarter'] = time_dim['Date'].dt.to_period('Q')
    time_dim['Month'] = time_dim['Date'].dt.month_name()
    time_dim['Week'] = time_dim['Date'].dt.isocalendar().week
    time_dim['Day'] = time_dim['Date'].dt.day
    time_dim['Weekday'] = time_dim['Date'].dt.day_name()

    # Add fiscal year/quarter if applicable
    time_dim['FiscalYear'] = time_dim['Year']  # Can be customized based on fiscal year
    time_dim['FiscalQuarter'] = time_dim['Quarter'].astype(str)

    # Step 5: Create a surrogate TimeKey (integer)
    time_dim['TimeKey'] = range(
        last_surrogates_keys['TimeKey'] + 1, 
        last_surrogates_keys['TimeKey'] + len(time_dim) + 1
    )

    # Update last_surrogates_keys
    last_surrogates_keys['TimeKey'] = time_dim['TimeKey'].max()

    # Reorder columns to match the time_dim schema
    reordered_col = ['TimeKey', 'Date', 'Year', 'Quarter', 'Month', 'Week', 'Day', 'Weekday', 'FiscalYear', 'FiscalQuarter']
    time_dim = time_dim[reordered_col]

    # Return the final time_dim dataframe and updated surrogate keys
    return time_dim, last_surrogates_keys




def create_sales_fact_table(transformed_tables, time_dim, customer_dim, product_dim, last_surrogates_keys):
    # Step 1: Get sales data from 'salesorder' and 'salesorderdetail' tables
    sales = transformed_tables['salesorder'].drop(columns=['totalamount'])
    sales_fct = pd.merge(sales, transformed_tables['salesorderdetail'], on='orderid', how='inner')

    # Step 2: Convert 'orderdate' to datetime format
    sales_fct['OrderDate'] = pd.to_datetime(sales_fct['orderdate'])

    # Step 3: Merge TimeKey from time_dim
    sales_fct = pd.merge(sales_fct, time_dim[['Date', 'TimeKey']], left_on='OrderDate', right_on='Date', how='left')

    # Step 4: Merge CustomerKey from customer_dim
    sales_fct = pd.merge(sales_fct, customer_dim[['CustomerID', 'CustomerKey']], left_on='customerid', right_on='CustomerID', how='left')

    # Step 5: Merge ProductKey from product_dim
    sales_fct = pd.merge(sales_fct, product_dim[['ProductID', 'ProductKey']],  left_on='productid', right_on='ProductID', how='left')

    # Step 6: Select relevant columns for the final sales_fct
    sales_fct = sales_fct[['TimeKey', 'CustomerKey', 'ProductKey', 'quantity', 'unitprice', 'discount', 'tax', 'totalamount']]

    # Step 7: Rename columns to align with the fact table schema
    sales_fct.columns = ['TimeKey', 'CustomerKey', 'ProductKey', 'Quantity', 'UnitPrice', 'Discount', 'Tax', 'TotalAmount']

    # Step 8: Return the final sales_fct table
    return sales_fct




def create_purchase_fact_table(transformed_tables, time_dim, supplier_dim, product_dim):
    # Step 1: Join 'purchaseorder' and 'purchaseorderdetail' on 'orderid'
    purchaseorder = transformed_tables['purchaseorder'].drop(columns=['totalamount'])
    purchase_fct = pd.merge(purchaseorder, transformed_tables['purchaseorderdetail'], on='orderid', how='inner')

    # Step 2: Convert 'orderdate' to datetime format and map TimeKey from time_dim
    purchase_fct['OrderDate'] = pd.to_datetime(purchase_fct['orderdate'])
    purchase_fct = pd.merge(purchase_fct, time_dim[['Date', 'TimeKey']], left_on='OrderDate', right_on='Date', how='left')

    # Step 3: Map SupplierKey from supplier_dim
    purchase_fct = pd.merge(
        purchase_fct, 
        supplier_dim[['SupplierID', 'SupplierKey']], 
        left_on='supplierid', right_on='SupplierID', 
        how='left', 
        indicator=True
    )
    purchase_fct = purchase_fct[purchase_fct['_merge'] == 'both']  # Retain only matches
    purchase_fct.drop(columns=['supplierid', 'SupplierID', '_merge'], inplace=True)

    # Step 4: Map ProductKey from product_dim
    purchase_fct = pd.merge(
        purchase_fct, 
        product_dim[['ProductID', 'ProductKey']], 
        left_on='productid', right_on='ProductID', 
        how='left', 
        indicator=True
    )
    purchase_fct = purchase_fct[purchase_fct['_merge'] == 'both']  # Retain only matches
    purchase_fct.drop(columns=['productid', 'ProductID', '_merge'], inplace=True)

    # Step 5: Select relevant columns for the final purchase_fct
    purchase_fct = purchase_fct[['TimeKey', 'SupplierKey', 'ProductKey', 'quantity', 'unitprice', 'discount', 'tax', 'totalamount']]

    # Step 6: Rename columns to align with the fact table schema
    purchase_fct.columns = ['TimeKey', 'SupplierKey', 'ProductKey', 'Quantity', 'UnitPrice', 'Discount', 'Tax', 'TotalAmount']

    # Step 7: Return the final purchase_fct table
    return purchase_fct





def create_inventory_fact_table(transformed_tables, product_dim, warehouse_dim):
    # Step 1: Load the 'inventory' table
    inventory_fct = transformed_tables['inventory']

    # Step 2: Merge with 'product' and 'warehouse' tables
    inventory_fct = pd.merge(inventory_fct, transformed_tables['product'], left_on='productid', right_on='productid', how='inner')
    inventory_fct = pd.merge(inventory_fct, transformed_tables['warehouse'], left_on='warehouseid', right_on='warehouseid', how='inner')

    # Step 3: Map ProductKey from product_dim and WarehouseKey from warehouse_dim
    inventory_fct = pd.merge(inventory_fct, product_dim, left_on='productid', right_on='ProductID', how='inner')
    inventory_fct = pd.merge(inventory_fct, warehouse_dim, left_on='warehouseid', right_on='WarehouseID', how='inner')

    # Step 4: Select relevant columns for the final inventory_fct
    inventory_fct = inventory_fct[['ProductKey', 'WarehouseKey', 'quantity', 'minimumstocklevel', 'maximumstocklevel', 'reorderpoint']]

    # Step 5: Rename columns to match the fact table schema
    inventory_fct.columns = ['ProductKey', 'WarehouseKey', 'Quantity', 'MinimumStockLevel', 'MaximumStockLevel', 'ReorderPoint']

    # Step 6: Return the final inventory_fct table
    return inventory_fct



def create_return_fact_table(transformed_tables, customer_dim, product_dim, time_dim):
    # Step 1: Merge 'returns' and 'returndetail' on 'returnid'
    return_fct = pd.merge(transformed_tables['returns'], transformed_tables['returndetail'], on='returnid', how='inner')

    # Step 2: Merge with 'customer' and 'product' tables to access additional details
    return_fct = pd.merge(return_fct, transformed_tables['customer'], left_on='customerid', right_on='customerid', how='inner')
    return_fct = pd.merge(return_fct, transformed_tables['product'], left_on='productid', right_on='productid', how='inner')

    # Step 3: Map CustomerKey from customer_dim and ProductKey from product_dim
    return_fct = pd.merge(return_fct, customer_dim[['CustomerID', 'CustomerKey']], left_on='customerid', right_on='CustomerID', how='inner')
    return_fct = pd.merge(return_fct, product_dim[['ProductID', 'ProductKey']], left_on='productid', right_on='ProductID', how='inner')

    # Step 4: Map TimeKey from time_dim based on 'ReturnDate'
    return_fct['ReturnDate'] = pd.to_datetime(return_fct['returndate'])
    return_fct = pd.merge(return_fct, time_dim[['Date', 'TimeKey']], left_on='ReturnDate', right_on='Date', how='left')

    # Step 5: Select and rename relevant columns to match the fact table structure
    return_fct = return_fct[['TimeKey', 'CustomerKey', 'ProductKey', 'quantity', 'totalamount', 'refundamount']]
    return_fct.columns = ['TimeKey', 'CustomerKey', 'ProductKey', 'Quantity', 'TotalAmount', 'RefundAmount']

    # Step 6: Return the final return_fct table
    return return_fct

