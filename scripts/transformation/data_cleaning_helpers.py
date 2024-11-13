
import pandas as pd

#HELPER FUNCTION
def remove_null_primary_keys(df, primary_keys):
    df_cleaned = df.dropna(subset=primary_keys)
    return df_cleaned

def remove_duplicates(df, primary_keys):
    df_cleaned = df.drop_duplicates(subset=primary_keys)
    return df_cleaned
def drop_columns(df, columns_to_drop):
    df_cleaned = df.drop(columns=columns_to_drop, errors='ignore')
    return df_cleaned

def fill_missing_numeric(df, numeric_fields, fill_value=0):
    df_cleaned = df.copy()
    for field in numeric_fields:
        df_cleaned[field] = df_cleaned[field].fillna(fill_value)
    return df_cleaned

def fill_missing_text(df, text_fields, fill_value="Unknown"):
    df_cleaned = df.copy()
    for field in text_fields:
        df_cleaned[field] = df_cleaned[field].fillna(fill_value)
    return df_cleaned

def round_numeric_columns(df, numeric_fields, decimals=2):
    df_cleaned = df.copy()
    for field in numeric_fields:
        df_cleaned[field] = df_cleaned[field].round(decimals)
    return df_cleaned

def format_dates(df, date_fields):
    df_cleaned = df.copy()

    for field in date_fields:
        if field in df_cleaned.columns:
            df_cleaned[field] = pd.to_datetime(df_cleaned[field], errors='coerce')
        else:
            print(f"Warning: Column '{field}' does not exist in the DataFrame.")
    
    return df_cleaned
