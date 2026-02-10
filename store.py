import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
import pyarrow as pa
import pyarrow.parquet as pq

from transform import transform


#df = transform('data/transformed/cars_transformed_20260131_052019.csv')

def load_transformed(df: pd.DataFrame):
    return pd.read_csv('data/transformed/cars_transformed_20260131_052019.csv')


def add_ingestion_time(df: pd.DataFrame):
    df['ingest_date'] = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return df

def to_parquet(df: pd.DataFrame):
    path =Path('data/analytics')
    path.parent.mkdir(parents=True,exist_ok=True)

    table = pa.table.from_pandas(df,preserve_index=False)

    parquet = pq.write_to_dataset(
        table,
        root_path='data/analytics',
        partition_cols=['ingest_date']
    )