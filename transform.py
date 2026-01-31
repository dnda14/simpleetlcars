import pandas as pd

from pathlib import Path
from datetime import datetime,timezone

def load_csv(path_csv):
    return pd.read_csv(path_csv)

def normalize(df:pd.DataFrame):
    cols_df = df.select_dtypes(include='str').columns # object is deprecater
    for col in cols_df:
        df[col] = df[col].str.strip().str.lower()

    return df

def convert_types(df:pd.DataFrame):
    int_cols = ['Year','Engine HP', 'Engine Cylinders','Number of Doors','highway MPG','city mpg','Popularity','MSRP']
    for col in int_cols:
        if col in df.columns:
            df[col] = df[col].astype('Int64')
            
    return df

def rich_df(df:pd.DataFrame):
    year_now = datetime.now(timezone.utc).year
    df['Age'] = year_now - df['Year']
    df['consume_eff'] = df['highway MPG']/df['city mpg']
    return df

def deduplicate(df:pd.DataFrame):
    len_i = len(df)
    df = df.drop_duplicates(
        subset=['Make','Model','Year','Engine HP'],
        keep='first')
    n_dropped = len_i - len(df)
    
    print(f"{n_dropped} rows was dropped.")
    return df

def apply_rules(df: pd.DataFrame):
    df= df[df['Age'] <= 40]
    df = df[df['consume_eff'] >0]
    
    return df

def transform(path):
    path = Path(path)
    df = load_csv(path)
    df = normalize(df)
    df = convert_types(df)
    df = rich_df(df)
    df = deduplicate(df)
    df = apply_rules(df)
    
    DIR_TRANS = Path('data/transformed')
    DIR_TRANS.mkdir(parents=True,exist_ok=True)
    batch_id =datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    df.to_csv(DIR_TRANS/f"cars_transformed_{batch_id}.csv",index=False)
    
    return df
    

print(transform('data/cleaned/cars_clean_20260128_115136.csv'))
