import logging
from pathlib import Path

def log_register():
    log_dir = Path('log')
    log_dir.mkdir(exist_ok=True)

    log_data = logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s',
        handlers=[
            logging.FileHandler('log/pipeline.log'),
            logging.StreamHandler()
            
        ]

    )
    