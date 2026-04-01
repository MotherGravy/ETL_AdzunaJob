import logging
import pandas as pd
import uuid

from extract import extract
from transform import transform
from load import load
from utils import retry

# логирование
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(message)s]"
)

logger = logging.getLogger(__name__)

def main():
    etl_id = str(uuid.uuid4())
    logger.info(f"[{etl_id}] ETL started")

    try:
        # --- Extract ---
        df_raw = retry(lambda: extract())
        logger.info(f"[{etl_id}] Extracted {len(df_raw)} rows")

        if df_raw.empty:
            logger.info(f"[{etl_id}] No new data. ETL finished")
            return

        # --- Transform ---
        df_transformed = transform(df_raw, etl_id)
        logger.info(f"[{etl_id}] Transform completed")

        # --- Load ---
        df_transformed.to_parquet(f"data/jobs_{etl_id}.parquet", compression="gzip", index=False)
        logger.info(f"[{etl_id}] Saved transformed data to Parquet")
        
        retry(lambda: load(df_transformed))
        logger.info(f"[{etl_id}] Load completed")

        logger.info(f"[{etl_id}] ETL finished successfully")

    except Exception as e:
        logger.error(f"[{etl_id}] ETL failed: {e}")
        raise


if __name__ == "__main__":
    main()