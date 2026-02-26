import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
import pyarrow as pa
import pyarrow.parquet as pq

import logging
from logger_config import log_register

log_register()
logger = logging.getLogger(__name__) #to get the file name

#df = transform('data/transformed/cars_transformed_20260131_052019.csv')

def load_transformed(path:Path):
    return pd.read_csv(path)


def add_ingestion_time(df: pd.DataFrame):
    df['ingest_date'] = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return df

def to_parquet(df: pd.DataFrame):
    path =Path('data/analytics')
    path.mkdir(parents=True,exist_ok=True)

    table = pa.Table.from_pandas(df,preserve_index=False)

    parquet = pq.write_to_dataset(
        table,
        root_path='data/analytics',
        partition_cols=['ingest_date']
    )

def validate_parquet():
    parquet = pq.ParquetDataset('data/analytics')
    
    table = parquet.read()

    assert table.num_rows > 0, 'is empty'

def run_storage(trfd_file:Path):
    logger.info('starting transformation')
    df = load_transformed(trfd_file)
    logger.info(f'laoded files with {len(df)} rows')
    
    df = add_ingestion_time(df)
    
    to_parquet(df)
    logger.info('parqeut written')

    validate_parquet()
    logger.info('parquet validated')
    logger.info('phase completed')


run_storage(Path('data/transformed/cars_transformed_20260131_052019.csv'))