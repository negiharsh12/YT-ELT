import json
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def load_data():
    file_path = f"./data/YT_data_{datetime.today().strftime('%Y-%m-%d')}.json"

    try:
        logger.info(f"Loading data from YT_data_{datetime.today().strftime('%Y-%m-%d')}")

        with open(file_path, 'r', encoding='utf-8') as raw_data:
            data = json.load(raw_data)
            
        logger.info(f"Data loaded successfully from {file_path}")
        return data
    
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON file: {file_path}")
        raise