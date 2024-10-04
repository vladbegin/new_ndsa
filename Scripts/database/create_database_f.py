import logging
import os
import sqlite3
from datetime import datetime
from dateutil import parser
import pandas as pd
from sqlalchemy import create_engine


def create_db(db_name):
    if not os.path.exists(db_name):
        conn = sqlite3.connect(db_name)
        cur = conn.cursor()

        conn.commit()
        conn.close()
        logging.info(f"Database '{db_name}' create.")
    else:
        logging.info(f"Database '{db_name}' is exists.")

def insert_new_records(df, table_name, unique_cols, database_file):
    conn = sqlite3.connect(database_file)
    cursor = conn.cursor()
    unique_cols_str = ', '.join(unique_cols)
    existing_ids_query = f"SELECT {unique_cols_str} FROM {table_name}"
    cursor.execute(existing_ids_query)
    existing_ids = {tuple(row) for row in cursor.fetchall()}
    new_records = []
    for _, row in df.iterrows():
        record = tuple(row[col] for col in unique_cols)
        if record not in existing_ids:
            new_records.append(row)
            existing_ids.add(record)
    if new_records:
        new_records_df = pd.DataFrame(new_records)
        engine = create_engine(f'sqlite:///{database_file}')
        new_records_df.to_sql(table_name, con=engine, if_exists='append', index=False)
        logging.info("Новые данные успешно загружены в базу данных SQLite!")
    else:
        logging.info("Нет новых данных для загрузки.")
    conn.close()

def get_max_date_from_table(table_name, column_name, database_file):
    logging.info(f"get max date from table {table_name}")
    conn = sqlite3.connect(database_file)
    # Создание движка для подключения к PostgreSQL
    # engine = create_engine(database_file)
    cursor = conn.cursor()
    query = f"SELECT MAX({column_name}) FROM {table_name}"
    cursor.execute(query)
    max_date_str = cursor.fetchone()[0]
    if max_date_str:
        try:
            # Пробуем разобрать строку как полную дату с временем и смещением часового пояса
            max_date = parser.isoparse(max_date_str).date()
        except ValueError:
            try:
                # Если не получилось, пробуем разобрать строку как дату с временем без смещения часового пояса
                max_date = datetime.strptime(max_date_str, '%Y-%m-%d %H:%M:%S').date()
            except ValueError:
                try:
                    # Если не получилось, пробуем разобрать строку как просто дату
                    max_date = datetime.strptime(max_date_str, '%Y-%m-%d').date()
                except ValueError as ex:
                    logging.error(f"Ошибка при преобразовании даты: {ex}")
                    max_date = None
    else:
        max_date = None

    cursor.close()
    conn.close()
    return max_date