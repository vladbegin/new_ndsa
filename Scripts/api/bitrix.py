import os
from dotenv import load_dotenv

from fast_bitrix24 import Bitrix

import pandas as pd
import sqlite3
from sqlalchemy import create_engine
def fetch_and_store_bitrix_leads(db_path, start_date):
    # Загрузка переменных окружения
    load_dotenv()

    # Получение вебхука из переменных окружения
    webhook = os.getenv('bitrix_url')
    bx = Bitrix(webhook)

    # Получение списка лидов
    leads = bx.get_all('crm.lead.list')
    df_leads = pd.DataFrame(leads)

    # Преобразование столбца DATE_CREATE в datetime
    df_leads['DATE_CREATE'] = pd.to_datetime(df_leads['DATE_CREATE'])

    # Фильтрация по дате создания
    filter_df = df_leads[df_leads['DATE_CREATE'].dt.date >= start_date]

    # Создание SQLAlchemy engine для SQLite базы данных
    engine = create_engine(f'sqlite:///{db_path}', connect_args={'check_same_thread': False})

    # Сохранение данных в базу данных
    filter_df.to_sql(name='bitrix_lead_list', con=engine, if_exists='replace', index=False)

