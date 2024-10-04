import os
import time
from datetime import timedelta, datetime

import pandas as pd
from dotenv import load_dotenv
import requests
import logging

from sqlalchemy import create_engine

from Scripts.database.create_database_f import insert_new_records

logging.basicConfig(level=logging.INFO, filename="py_log.log",filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s", encoding='utf-8')


load_dotenv()
# Установите заголовки для авторизации
API_TOKEN = os.getenv('YANDEX_ACCESS_TOKEN')
headers = {
    'Authorization': f'OAuth {API_TOKEN}',}

def fetch_data_for_date(date, counter_id, metrics, dimensions):
    params = {
        'id': counter_id,
        'metrics': metrics,
        'date1': date.strftime('%Y-%m-%d'),
        'date2': date.strftime('%Y-%m-%d'),
        'dimensions': dimensions,
        'accuracy': 'full',
        'limit': 10000
    }
    # logging.info(date.strftime('%Y-%m-%d'))
    try:
        response = requests.get('https://api-metrika.yandex.net/stat/v1/data', headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f'Ошибка при запросе данных за {date.strftime("%Y-%m-%d")}: {e}')
        return None


def fetch_and_store_data(start_date, end_date, counter_id, metrics, dimensions, table_name, unique_cols,
                         filter_condition=None, database_file=os.getenv('db_name')):
    all_data = pd.DataFrame()
    current_date = start_date
    logging.info(f'Начало получения данных для {table_name}')

    while current_date <= end_date:
        time.sleep(2)
        # logging.info(f'Получение данных за дату: {current_date.strftime("%Y-%m-%d")}')
        data = fetch_data_for_date(current_date, counter_id, metrics, dimensions)

        if data and 'data' in data:
            try:
                df = pd.json_normalize(data['data'])
                dimension_names = [dim.split(':')[-1] for dim in data['query']['dimensions']]
                metric_names = [met.split(':')[-1] for met in data['query']['metrics']]

                for i, dim_name in enumerate(dimension_names):
                    df[dim_name] = df['dimensions'].apply(
                        lambda x: x[i]['name'] if isinstance(x, list) and len(x) > i else None)

                df[metric_names] = pd.DataFrame(df['metrics'].tolist(), index=df.index)
                df = df.drop(columns=['dimensions', 'metrics'])
                df.columns = dimension_names + metric_names
                all_data = pd.concat([all_data, df], ignore_index=True)

            except Exception as e:
                logging.error(f'Ошибка при обработке данных за {current_date.strftime("%Y-%m-%d")}: {e}')

        current_date += timedelta(days=1)

    if filter_condition:
        all_data = all_data.query(filter_condition)

    try:
        engine = create_engine(f'sqlite:///{database_file}', connect_args={'check_same_thread': False})
        # all_data.to_sql(table_name, con=engine, if_exists='replace', index=False)
        insert_new_records(all_data, table_name, unique_cols, database_file)
        logging.info(f"Данные успешно загружены в таблицу {table_name} базы данных SQLite!")
    except Exception as e:
        logging.error(f'Ошибка при загрузке данных в базу данных: {e}')
