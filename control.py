import csv
from pathlib import Path

CONTROL_FILE = "data/metadata/processed_files.csv"

def load_processed_batches():
    with open(CONTROL_FILE) as f:
        reader = csv.DictReader(f)
        return {row['batch_id'] for row in reader}

def is_batch_processed(batch_id:str) -> bool:
    processed = load_processed_batches()
    return batch_id in processed
    