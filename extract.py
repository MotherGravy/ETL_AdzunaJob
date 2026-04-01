import requests
import pandas as pd
import logging
from sqlalchemy import create_engine
from config import API_ID, API_KEY, COUNTRY, DB_CONFIG

logger = logging.getLogger(__name__)

def get_last_ids_from_db(limit=1000):
    try:
        engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        query = f"SELECT id FROM adzuna_jobs ORDER BY created DESC LIMIT {limit}"
        df = pd.read_sql(query, engine)
        return set(df['id'].tolist())
    except Exception as e:
        logger.warning(f"Не удалось получить последние id из БД: {e}")
        return set()

def extract(max_pages=5):
    all_jobs = []
    page = 1

    existing_ids = get_last_ids_from_db()

    while page <= max_pages:
        url = (
            f"https://api.adzuna.com/v1/api/jobs/{COUNTRY}/search/{page}"
            f"?app_id={API_ID}&app_key={API_KEY}&results_per_page=50"
        )

        try:
            response = requests.get(url, timeout=5)

            if response.status_code != 200:
                logger.error(f"Page {page}: bad response {response.status_code}")
                break

            data = response.json()
            results = data.get("results", [])

            if not results:
                logger.info(f"Page {page}: no more data, stopping")
                break

            # Фильтруем только новые вакансии
            new_results = [r for r in results if r.get('id') not in existing_ids]

            if not new_results:
                logger.info(f"Page {page}: all jobs already loaded, stopping")
                break

            all_jobs.extend(new_results)
            logger.info(f"Page {page}: loaded {len(new_results)} new rows")

            page += 1

        except requests.RequestException as e:
            logger.error(f"Page {page}: request failed: {e}")
            raise

    df = pd.DataFrame(all_jobs)
    logger.info(f"Total extracted new rows: {len(df)}")

    return df