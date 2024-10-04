import logging
import re

import gspread
import numpy as np
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine


def load_data_from_google_sheets(credentials_file, spreadsheet_url, worksheet_name, expected_headers, numeric_columns, table_name, database_file):
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]

    # Авторизация и создание клиента gspread
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
    client = gspread.authorize(credentials)

    # Откройте таблицу по URL
    spreadsheet = client.open_by_url(spreadsheet_url)

    # Откройте лист
    worksheet = spreadsheet.worksheet(worksheet_name)

    # Получите все данные с листа
    data = worksheet.get_all_values()

    # Преобразуйте данные в DataFrame
    df = pd.DataFrame(data[1:], columns=data[0])  # Пропускаем первую строку с заголовками

    # Вывод типов данных всех столбцов
    # print("Типы данных всех столбцов:")
    # print(df.dtypes)

    # Проверка каждого столбца на наличие значений-списков
    for column in df.columns:
        for value in df[column]:
            if isinstance(value, list):
                # print(f"Столбец '{column}' содержит значение-список: {value}")
                logging.info(f"Столбец '{column}' содержит значение-список: {value}")

                break

    # Проверка наличия ожидаемых заголовков
    missing_headers = [header for header in expected_headers if header not in df.columns]
    if missing_headers:
        raise ValueError(f"Missing expected headers: {missing_headers}")

    # Преобразование чисел с запятой в числа с плавающей точкой
    for column in numeric_columns:
        if column in df.columns:
            df[column] = df[column].replace('', np.nan)  # Заменить пустые строки на NaN
            df[column] = df[column].astype(str).str.replace(u'\xa0', '').str.replace(',', '.').str.replace(' ', '').astype(float)

    # Фильтруйте DataFrame, оставив только необходимые столбцы
    filtered_df = df[expected_headers]

    # Показать первые несколько строк для проверки
    # print(filtered_df.head())
    # print(df.dtypes)

    # Подключение к базе данных SQLite
    engine = create_engine(f'sqlite:///{database_file}')

    # Загрузка данных в таблицу SQLite
    try:
        filtered_df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        # print("Данные успешно загружены в базу данных SQLite!")
        logging.info(f"Данные успешно загружены в таблицу {table_name} базы данных SQLite!")
    except Exception as ex:
        print(ex)


credentials_file = 'config/nimble-hash-276219-1c965d6cdc2e.json'


def get_status_lead_from_google_sheet(credentials_file, spreadsheet_url, worksheet_name, table_name, database_file):
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]

    # Авторизация и создание клиента gspread
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
    client = gspread.authorize(credentials)

    # Откройте таблицу по URL
    spreadsheet = client.open_by_url(spreadsheet_url)
    # Откройте лист
    worksheet = spreadsheet.worksheet(worksheet_name)
    # Получите все данные с листа
    data = worksheet.get_all_values()
    # Преобразование данных в DataFrame
    columns = data[0]  # Первая строка данных - это заголовки
    rows = data[1:]  # Остальные строки - это данные
    df = pd.DataFrame(rows, columns=columns)
    # Обработка столбца с проверкой на наличие результата поиска
    df['lead_id'] = df['Ссылка на лид'].apply(
        lambda x: re.search(r'details/(\d+)', x).group(1) if re.search(r'details/(\d+)', x) else None)

    # Подключение к базе данных SQLite
    engine = create_engine(f'sqlite:///{database_file}')

    # Загрузка данных в таблицу SQLite
    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        # print("Данные успешно загружены в базу данных SQLite!")
        logging.info(f"Данные успешно загружены в таблицу {table_name} базы данных SQLite!")
    except Exception as ex:
        print(ex)
