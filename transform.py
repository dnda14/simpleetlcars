import pandas as pd

from pathlib import Path
from datetime import datetime

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
            
normalize(load_csv(Path('data/cleaned/cars_clean_20260128_115136.csv')))
