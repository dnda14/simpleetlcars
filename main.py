from control import is_batch_processed, mark_batch_success
from transform import transform
from validate import run_validation

from pathlib import Path
import logging

PATH = Path('dsd')
batch_id ='333'
try:
    if is_batch_processed(batch_id):
        run_validation(PATH,batch_id)
        transform(PATH)
        mark_batch_success(batch_id)
except Exception as e:
    logging.error(f"{batch_id} failed: {e}")
    raise

    
    
    