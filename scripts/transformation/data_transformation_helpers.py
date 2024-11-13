import pandas as pd
from .data_cleaning_helpers import *

# Category Transformation
def transform_category(df):
    df = remove_null_primary_keys(df, ['categoryid'])
    df = remove_duplicates(df, ['categoryid'])
    
    missing_names = df[df['name'].isna()]
    for idx, row in missing_names.iterrows():
        matching_row = df[df['description'] == row['description']]
        if not matching_row.empty:
            df.at[idx, 'name'] = matching_row.iloc[0]['name']
    
    df = fill_missing_text(df, ['name'])
    df = drop_columns(df, ['description'])
    
    return df


# Customer Transformation
def transform_customer(df):
    df = remove_null_primary_keys(df, ['customerid'])
    df = remove_duplicates(df, ['customerid'])
    
    df = fill_missing_text(df, ['preferredpaymentmethod', 'accountstatus', 'address'])
    df = fill_missing_numeric(df, ['creditlimit'], fill_value=df['creditlimit'].median())
    df = round_numeric_columns(df, ['creditlimit'])
    df = drop_columns(df, ['contactinfo', 'email'])
    
    return df

# Department Transformation
def transform_department(df):
    df = remove_null_primary_keys(df, ['departmentid'])
    df = remove_duplicates(df, ['departmentid'])
    
    df = fill_missing_numeric(df, ['budget'], fill_value=df['budget'].median())
    df = fill_missing_text(df, ['name'])
    df = round_numeric_columns(df, ['budget'])
    
    return df

# Employee Transformation
def transform_employee(df):
    df = remove_null_primary_keys(df, ['employeeid'])
    df = remove_duplicates(df, ['employeeid'])
    
    medians = df.groupby(['roleid'])[['salary', 'commission']].median()
    medians['commission'].fillna(value=medians['commission'].median(), inplace=True)
    df = df.merge(medians, on='roleid', suffixes=('', '_median'))
    
    df['salary'].fillna(df['salary_median'], inplace=True)
    df['commission'].fillna(df['commission_median'], inplace=True)
    
    df = drop_columns(df, ['name', 'contactinfo', 'salary_median', 'commission_median'])
    df = round_numeric_columns(df, ['salary', 'commission'])
    df = format_dates(df, ['hiredate'])
    
    return df

# Inventory Transformation
def transform_inventory(df):
    df = remove_null_primary_keys(df, ['productid', 'warehouseid'])
    df = remove_duplicates(df, ['productid', 'warehouseid'])
    
    df = format_dates(df, ['lastreorderdate'])
    df['lastreorderdate'] = df['lastreorderdate'].fillna(pd.Timestamp('1900-01-01'))
    
    df = format_dates(df, ['expecteddeliverydate', 'lastreorderdate'])
    
    df['reorderpoint'] = df.groupby(['warehouseid'])['reorderpoint'].transform(lambda x: x.fillna(x.mean()))
    df['minimumstocklevel'] = df.groupby(['warehouseid', 'productid'])['minimumstocklevel'].transform(lambda x: x.fillna(x.mean()))
    df['maximumstocklevel'] = df.groupby(['warehouseid', 'productid'])['maximumstocklevel'].transform(lambda x: x.fillna(x.mean()))
    
    df['reorderpoint'] = df['reorderpoint'].astype('int')
    
    return df

# Location Transformation
def transform_location(df):
    df = remove_null_primary_keys(df, ['locationid'])
    df = remove_duplicates(df, ['locationid'])
    
    df = drop_columns(df, ['latitude', 'longitude', 'postalcode'])
    df = fill_missing_text(df, ['country', 'region', 'city'])
    
    return df

# Manufacturer Transformation
def transform_manufacturer(df):
    df = remove_null_primary_keys(df, ['manufacturerid'])
    df = remove_duplicates(df, ['manufacturerid'])
    
    df = drop_columns(df, ['contactinfo', 'email'])
    df = fill_missing_text(df, ['name', 'address', 'country', 'phone'])
    
    return df

# Payment Transformation
def transform_payment(df):
    df = remove_null_primary_keys(df, ['paymentid'])
    df = remove_duplicates(df, ['paymentid'])
    
    textual = ['paymentmethod', 'status']
    numeric = ['amount', 'confirmationnumber']
    
    df = fill_missing_text(df, textual)
    df = fill_missing_numeric(df, numeric)
    df = format_dates(df, ['paymentdate'])
    df = round_numeric_columns(df, ['amount'])
    
    return df

# Product Transformation
def transform_product(df):
    df = remove_null_primary_keys(df, ['productid'])
    df = remove_duplicates(df, ['productid'])
    
    df = fill_missing_text(df, ['name', 'discontinued'])
    df = fill_missing_numeric(df, ['price', 'stocklevel', 'reorderlevel'])
    df = round_numeric_columns(df, ['price', 'reorderlevel'])
    df = drop_columns(df, ['description'])
    
    return df

# Purchase Order Transformation
def transform_purchaseorder(df):
    df = remove_null_primary_keys(df, ['orderid'])
    df = remove_duplicates(df, ['orderid'])
    
    df = fill_missing_text(df, ['status', 'paymentmethod', 'paymentstatus'])
    df = fill_missing_numeric(df, ['totalamount'], fill_value=0)
    df = format_dates(df, ['expecteddeliverydate', 'actualdeliverydate'])
    df = round_numeric_columns(df, ['totalamount'])
    df = drop_columns(df, ['comments'])
    
    return df

# Purchase Order Detail Transformation
def transform_purchaseorderdetail(df):
    df = remove_null_primary_keys(df, ['orderid', 'productid'])
    df = remove_duplicates(df, ['orderid', 'productid'])
    
    df = fill_missing_numeric(df, ['quantity', 'tax', 'unitprice'], fill_value=0)
    df = fill_missing_text(df, ['deliverystatus'])
    df = round_numeric_columns(df, ['unitprice'])
    
    return df

# Return Detail Transformation
def transform_returndetail(df):
    df = remove_null_primary_keys(df, ['returnid', 'productid'])
    df = remove_duplicates(df, ['returnid', 'productid'])
    
    df = fill_missing_numeric(df, ['quantity', 'unitprice', 'discount', 'tax', 'totalamount'], fill_value=0)
    
    return df

# Returns Transformation
def transform_returns(df):
    df = remove_null_primary_keys(df, ['returnid'])
    df = remove_duplicates(df, ['returnid'])
    
    df = fill_missing_text(df, ['returndate', 'reason', 'refundmethod', 'refundstatus'])
    df = fill_missing_numeric(df, ['refundamount'], fill_value=0)
    df = format_dates(df, ['returndate'])
    df = round_numeric_columns(df, ['refundamount'])
    df = drop_columns(df, ['comments', 'reason'])
    
    return df

# Sales Order Transformation
def transform_salesorder(df):
    df = remove_null_primary_keys(df, ['orderid'])
    df = remove_duplicates(df, ['orderid'])
    
    df = fill_missing_text(df, ['status', 'paymentmethod', 'paymentstatus'])
    df = fill_missing_numeric(df, ['totalamount'], fill_value=0)
    df = format_dates(df, ['orderdate', 'expecteddeliverydate', 'actualdeliverydate'])
    
    df['expecteddeliverydate'] = df['expecteddeliverydate'].fillna(pd.Timestamp('1900-01-01'))
    df = round_numeric_columns(df, ['totalamount'])
    
    return df

# Sales Order Detail Transformation
def transform_salesorderdetail(df):
    df = remove_null_primary_keys(df, ['orderid', 'productid'])
    df = remove_duplicates(df, ['orderid', 'productid'])
    
    df = fill_missing_numeric(df, ['quantity', 'unitprice', 'tax', 'discount', 'totalamount'], fill_value=0)
    df = fill_missing_text(df, ['deliverystatus'])
    df = round_numeric_columns(df, ['unitprice', 'totalamount', 'tax'])
    
    return df

# Shipment Transformation
def transform_shipment(df):
    df = remove_null_primary_keys(df, ['shipmentid'])
    df = remove_duplicates(df, ['shipmentid'])
    
    df = fill_missing_text(df, ['status', 'carrier', 'trackingnumber'])
    df = format_dates(df, ['shipmentdate', 'estimatedarrivaldate', 'actualarrivaldate'])
    
    df['estimatedarrivaldate'] = df['estimatedarrivaldate'].fillna(pd.Timestamp('1900-01-01'))
    df['shipmentdate'] = df['shipmentdate'].fillna(pd.Timestamp('1900-01-01'))
    
    return df

# Shipment Detail Transformation
def transform_shipmentdetail(df):
    df = remove_null_primary_keys(df, ['shipmentid', 'productid'])
    df = remove_duplicates(df, ['shipmentid', 'productid'])
    
    df = fill_missing_numeric(df, ['quantity', 'unitprice', 'tax', 'discount', 'totalamount'], fill_value=0)
    
    return df

# Supplier Transformation
def transform_supplier(df):
    df = remove_null_primary_keys(df, ['supplierid'])
    df = remove_duplicates(df, ['supplierid'])

    # Fill Missing Data
    df = fill_missing_text(df, ['name', 'contactinfo','country'])
    df = fill_missing_numeric(df, ['rating'], fill_value=0)

    df = format_dates(df,['contractstartdate', 'contractenddate'])
    df['contractstartdate'] = df['contractstartdate'].fillna(pd.Timestamp('1900-01-01'))
    
    return df

# Warehouse Transformation
def transform_warehouse(df):
    df = remove_null_primary_keys(df, ['warehouseid'])
    df = remove_duplicates(df, ['warehouseid'])
    
    df = fill_missing_text(df, ['managerid', 'locationid', 'capacity'])
    
    return df