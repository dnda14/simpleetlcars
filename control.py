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

def mark_batch_success(batch_id:str):
    rows =[]
    
    with open(CONTROL_FILE)as f:
        reader = csv.DictReader(f)
        
        for i in reader:
            if i['batch_id'] == batch_id:
                i['status'] = 'success'
            rows.append(i)

    with open(CONTROL_FILE,'w',newline='') as f:
        
        writer = csv.DictWriter(f,
                                ['batch_id','file_name','checksum','processed_at','status'])
        writer.writeheader()
        writer.writerows(rows)
    