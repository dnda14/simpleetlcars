import pandas as pd

from pathlib import Path
from datetime import datetime,timezone

def load_csv(path_csv):
    return pd.read_csv(path_csv)

def normalize(df:pd.DataFrame):
    cols_df = df.select_dtypes(include='str').columns # object is deprecater
    for col in cols_df:
        df[col] = df[col].strip().lower()

    return df

def finalize_types(df:pd.DataFrame):
    int_cols = ['Year','Engine HP', 'Engine Cylinders','Number of Doors','highway MPG','city mpg','Popularity','MSRP']
    for col in int_cols:
        if col in df.columns:
            df[col] = df[col].astype('uint32')
            
    return df

def rich_df(df:pd.DataFrame):
    year_now = datetime.now(timezone.utc).year
    df['Age'] = year_now - df['Year']
    df['consume_eff'] = df['high MPG']/df['city mpg']
    return df

def deduplication(df:pd.DataFrame):
    len_i = len(df)
    df = df.drop_duplicates(
        subset=['Make','Model','Year','Engine HP'],
        keep='first')
    n_dropped = len_i - len(df)
    
    print(f"{n_dropped} rows was dropped.")
    return df
    
normalize(load_csv(Path('data/cleaned/cars_clean_20260128_115136.csv')))
