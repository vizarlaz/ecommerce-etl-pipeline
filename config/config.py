import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_DATA_DIR = os.path.join(BASE_DIR, 'data', 'raw')
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, 'data', 'processed')
WAREHOUSE_DATA_DIR = os.path.join(BASE_DIR, 'data', 'warehouse')


DATABASE_CONFIG = {
    'warehouse': {
        'type': 'sqlite',
        'path': os.path.join(WAREHOUSE_DATA_DIR, 'ecommerce_warehouse.db')
    }
}

LOG_DIR = os.path.join(BASE_DIR, 'logs')
LOG_FILE = os.path.join(LOG_DIR, 'etl_pipeline.log')

DATA_QUALITY_CONFIG = {
    'max_null_percentage': 10,
    'min_rows_threshold': 1
}