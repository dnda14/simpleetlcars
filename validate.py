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
        (df['Year']>=1950) &
        (df['Year']<=datetime.now(timezone.utc).year) &
        (df['MSRP']>0) &
        (df['highway MPG']> 0)  &
        (df['city mpg']>0) 
    )
    valid = df[conditions].copy() #apply filter mask
    invalid = df[~conditions].copy()
    
    return valid, invalid

def save_outputs(valid , invalid, batch_id):
    CLEAN_DIR = Path('data/cleaned')
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    
    REJECT_DIR = Path('data/rejected')
    REJECT_DIR.mkdir(parents=True, exist_ok=True)
    
    valid.to_csv(CLEAN_DIR / f"cars_clean_{batch_id}.csv")
    invalid.to_csv(REJECT_DIR / f"cars_reject_{batch_id}.csv")
    

def show_metrics(df,valid, invalid):
    return{
        " # rows": len(df),
        "# valid":len(valid),
        "# invalid":len(invalid),
        "invalid %":(len(invalid)/len(df))*100
    }

def run_validation(raw_file: Path, batch_id: str):
    df = read_raw(raw_file)
    verify_col(df)
    df = verify_datatype(df)
    valid, invalid = validate_rows(df)
    save_outputs(valid, invalid, batch_id)

    metrics = show_metrics(df, valid, invalid)
    print(metrics)
    
run_validation(Path('data/raw/cars_raw_20260128_115136.csv'), '20260128_115136')

    
    
    
