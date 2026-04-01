import pandas as pd
from sqlalchemy import create_engine
import logging
from config import DB_CONFIG

logger = logging.getLogger(__name__)

DATABASE_URI = f'postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}'

def load(data, table_name='Adzuna_jobs'):
    if data.empty:
        logger.warning("No data to load")
        return

    try:
        engine = create_engine(DATABASE_URI)
        data.to_sql(
            table_name,
            engine,
            if_exists='append',
            index=False,
            method='multi'
        )
        logger.info(f"Loaded {len(data)} rows into {table_name}")

    except Exception as e:
        logger.error(f"Failed to load data: {e}")
        raise