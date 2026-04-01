# ETL Project: Adzuna Jobs

## Описание
Проект реализует ETL для загрузки вакансий с API Adzuna в PostgreSQL и локально в Parquet.  

Функции ETL:
- **Extract:** забирает новые вакансии через API, фильтрует дубликаты по `id`.
- **Transform:** добавляет колонки, анализирует текст (Python, AI, удалённая работа), нормализует данные.
- **Load:** сохраняет данные в PostgreSQL и локально в `data/jobs_<etl_id>.parquet`.

---

## Структура проекта
etl_project/
├─ extract.py
├─ transform.py
├─ load.py
├─ main.py
├─ utils.py
├─ config.py
├─ requirements.txt
├─ README.md
├─ data/ # для parquet файлов

---

## Установка

1. Клонировать репозиторий:
```bash
git clone https://github.com/MotherGravy/ETL_AdzunaJob.git
cd ETL_AdzunaJob
pip install -r requirements.txt
```

2. Настроить config.py:
API_ID = "<ваш_api_id>"
API_KEY = "<ваш_api_key>"
COUNTRY = "gb"

DB_CONFIG = {
    "user": "<db_user>",
    "password": "<db_password>",
    "host": "localhost",
    "port": "5432",
    "database": "jobs_db"
}

3. Запуск ETL
python main.py

Данные загрузятся в PostgreSQL и локально в data/jobs_<etl_id>.parquet.

4. Замечания
Папка data/ должна существовать перед запуском. Если её нет, создайте:
mkdir data
