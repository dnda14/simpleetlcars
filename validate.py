import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

SCHEMA = {
    "Make": str,
    "Model": str,
    "Year": int,
    "Engine Fuel Type": str,
    "Engine HP": float,
    "Engine Cylinders": float,
    "Transmission Type": str,
    "Driven_Wheels": str,
    "Number of Doors": float,
    "Market Category": str,
    "Vehicle Size": str,
    "Vehicle Style": str,
    "highway MPG": float,
    "city mpg": float,
    "Popularity": int,
    "MSRP": int
}

def read_raw(path:Path) -> pd.DataFrame:
    return pd.read_csv(path)

def verify_col(df:pd.DataFrame):
    diff = set(SCHEMA.keys()) - set(df.columns)
    if diff:
        raise KeyError(f"missing {diff} columns")
    
def verify_datatype(df:pd.DataFrame):
    for col, tp in SCHEMA.items():
        if col in df.columns:
            df[col]=pd.to_numeric(df[col],errors='coerce') if tp != str else df[col].astype(str)    
    return df

def validate_rows(df: pd.DataFrame):
    conditions = (
        (df['year']>=1950) &
        (df['year']<=datetime.now(timezone.utc).year) &
        (df['MSRP']>0) &
        (df['highway MPG']> 0)  &
        (df['highway mpg']>0) 
    )
    valid = df[conditions].copy #apply filter mask
    invalid = df[~conditions].copy 
    
    return valid, invalid


    
