import hashlib
import shutil
from pathlib import Path
from datetime import datetime, timezone
import csv

SOURCE_FILE =Path('data/source/data.csv')

batch_id =datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

raw_file = RAW_DIR/f"cars_raw_{batch_id}.csv"
shutil.copy2(SOURCE_FILE,raw_file) # keep metadata

def file_checksum(path): # to verify integrity
    h = hashlib.sha256()
    with open(path,'rb') as f:
        for chunk in iter(lambda: f.read(8192),b""):
            h.update(chunk)
    
    return h.hexdigest()

checksum = file_checksum(raw_file)


CONTROL_FILE  = Path("data/metadata/processed_files.csv")




