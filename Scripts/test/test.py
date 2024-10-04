import logging
import os
import time
from time import sleep
from dateutil import parser
import requests
from dotenv import load_dotenv

from fast_bitrix24 import Bitrix
import pandas as pd
import sqlite3
from sqlalchemy import create_engine

from Scripts.database.create_database_f import insert_new_records


#
# def fetch_and_store_bitrix_leads_test(db_path, start_date):
#     try:
#         logging.info(f'Выгрузка заявок из битрикс')
#         # Начало отсчета времени
#         start_time = time.time()
#         # Загрузка переменных окружения
#         load_dotenv()
#
#         # Инициализация пустого DataFrame для сохранения всех данных
#         all_leads = pd.DataFrame()
#         fields = ['ID','DATE_CREATE','UTM_SOURCE','UTM_MEDIUM','UTM_CONTENT','UTM_CAMPAIGN','SOURCE_DESCRIPTION','TITLE','CREATED_BY_ID','LAST_NAME','NAME']
#         # Начальное значение для параметра start
#         start = 0
#
#         while True:
#             sleep(1)
#             params = {
#                 'order[DATE_CREATE]': 'DESC',
#                 'filter[>DATE_CREATE]': start_date,
#                 'select[]': fields,  # Обратите внимание на 'select[]'
#                 'start': start  # Начало списка, используется для пагинации
#             }
#
#             # Отправка запроса
#             response = requests.get('https://naslediedigital.bitrix24.ru/rest/845/7qaa60d241sqv61p/crm.lead.list.json',
#                                     params=params)
#
#             # Проверка успешности запроса
#             if response.status_code == 200:
#                 leads = response.json()
#                 # Преобразование данных в DataFrame и добавление к общему DataFrame
#                 leads_df = pd.DataFrame(leads['result'])
#                 # print(leads_df['DATE_CREATE'])
#                 all_leads = pd.concat([all_leads, leads_df], ignore_index=True)
#
#                 # Проверка, есть ли следующая страница данных
#                 if 'next' in leads:
#                     start = leads['next']
#                 else:
#                     break
#                 # Создание SQLAlchemy engine для SQLite базы данных
#                 engine = create_engine(f'sqlite:///{db_path}', connect_args={'check_same_thread': False})
#                 #
#                 # Сохранение данных в базу данных
#                 all_leads.to_sql(name='bitrix_lead_list', con=engine, if_exists='replace', index=False)
#
#                 # Завершение отсчета времени
#         end_time = time.time()
#         elapsed_time = end_time - start_time
#         # Запись времени выполнения в лог
#         logging.info(f"Таблица bitrix_lead_list успешно создана за {elapsed_time:.2f} секунд!")
#     except Exception as e:
#         logging.error(f"Ошибка при создании таблицы consultation_fb_table: {e}")


def fetch_and_store_bitrix_leads_test(database_file, start_date, table_name, unique_cols):
    try:
        logging.info(f'Выгрузка заявок из битрикс')
        # Начало отсчета времени
        start_time = time.time()
        # Загрузка переменных окружения
        load_dotenv()

        # Инициализация пустого DataFrame для сохранения всех данных
        all_leads = pd.DataFrame()
        fields = ['ID', 'DATE_CREATE', 'UTM_SOURCE', 'UTM_MEDIUM', 'UTM_CONTENT', 'UTM_CAMPAIGN','UTM_TERM', 'SOURCE_DESCRIPTION',
                  'TITLE', 'CREATED_BY_ID', 'LAST_NAME', 'NAME','STATUS_DESCRIPTION']
        # Начальное значение для параметра start
        start = 0

        while True:
            sleep(1)
            params = {
                'order[DATE_CREATE]': 'DESC',
                'filter[>DATE_CREATE]': start_date,
                'select[]': "*",  # Обратите внимание на 'select[]'
                'start': start  # Начало списка, используется для пагинации
            }

            # Отправка запроса
            response = requests.get('https://naslediedigital.bitrix24.ru/rest/845/7qaa60d241sqv61p/crm.lead.list.json',
                                    params=params)

            # Проверка успешности запроса
            if response.status_code == 200:
                leads = response.json()
                # Преобразование данных в DataFrame и добавление к общему DataFrame
                leads_df = pd.DataFrame(leads['result'])
                # print(leads_df['DATE_CREATE'])
                all_leads = pd.concat([all_leads, leads_df], ignore_index=True)

                # Проверка, есть ли следующая страница данных
                if 'next' in leads:
                    start = leads['next']
                    # print(start)
                else:
                    break  # Завершение цикла, если больше нет страниц с данными

            else:
                logging.error(f"Ошибка при получении данных: {response.status_code}")
                break

        # Создание SQLAlchemy engine для SQLite базы данных
        engine = create_engine(f'sqlite:///{database_file}', connect_args={'check_same_thread': False})
        #
        # Сохранение данных в базу данных перезапись всех данных
        # all_leads.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

        #Добавление данныех в таблицу
        insert_new_records(all_leads, table_name, unique_cols, database_file)

        # Завершение отсчета времени
        end_time = time.time()
        elapsed_time = end_time - start_time
        # Запись времени выполнения в лог
        logging.info(f"Таблица bitrix_lead_list успешно создана за {elapsed_time:.2f} секунд!")

    except Exception as e:
        logging.error(f"Ошибка при создании таблицы bitrix_lead_list: {e}")


def bitrix_leads_stage_preload(database_file, table_name):
    # Загрузить Excel файл
    # Создание SQLAlchemy engine для SQLite базы данных
    engine = create_engine(f'sqlite:///{database_file}', connect_args={'check_same_thread': False})
    file_path = 'C:\\Users\\vladimir\PycharmProjects\\001_Nalsed\data\\stage_bitrix.csv'
    df = pd.read_csv(file_path, sep=";")
    df_stage = df[['ID', 'Стадия']].copy()

    # Добавление колонки "статус" на основе условий
    # def determine_status(stage):
    #     if stage == 'Назначен ПШ':
    #         return 'Назначен ПШ'
    #     elif stage == 'Думают о покупке':
    #         return 'Пришли и выразили желание купить'
    #     elif stage == 'Неявка на ПШ':
    #         return 'Не явились на ПШ'
    #     elif stage == 'Отказ от покупки':
    #         return 'Пришли и не выразили желание купить'
    #     elif stage == 'Покупка':
    #         return 'Покупка'
    #     else:
    #         return None
    #
    # # Применение функции к столбцу 'Стадия'
    # df_stage.loc[:, 'статус'] = df_stage['Стадия'].apply(determine_status)
    #
    # # Запись в базу данных
    # df_stage.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

    df_stage.to_sql(name=table_name, con=engine, if_exists='replace', index=False)