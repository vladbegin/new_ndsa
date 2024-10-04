import logging
import os
import pandas as pd
from dotenv import load_dotenv
import sys

from sqlalchemy import create_engine
from tapi_yandex_direct import YandexDirect

from Scripts.database.create_database_f import insert_new_records

load_dotenv()



def get_data_from_yandex_direct(table_name,unique_cols,database_file):
    logging.info("Старт получения данных Яндекс Директ")

    ACCESS_TOKEN = os.getenv('YANDEX_DIRECT_TOKEN')
    client = YandexDirect(
     # Required parameters:
        access_token=ACCESS_TOKEN,
     # If you are making inquiries from an agent account, you must be sure to specify the account login.
        login=os.getenv('YANDEX_DIRECT_LOGIN'),
     # Optional parameters:
        # Enable sandbox.
        is_sandbox=False,
     # Repeat request when units run out
        retry_if_not_enough_units=False,
     # The language in which the data for directories and errors will be returned.
        language="ru",
     # Repeat the request if the limits on the number of reports or requests are exceeded.
        retry_if_exceeded_limit=True,
     # Number of retries when server errors occur.
        retries_if_server_error=5
    )

    # Создание тела запроса
    body = {
        "params": {
            "SelectionCriteria": {
                "Filter": [{
                    "Field": "CampaignId",
                    "Operator": "IN",
                    "Values": ["112838670", "112838956", "113426103", "113197310", "113672499","113672539", "114323290","114710421","114560912","114561074"]
                }]
            },
            "FieldNames": [
                "Date",
                "CampaignId",
                "CampaignType",
                "CampaignName",
                "AdGroupId",
                "AdGroupName",
                "AdId",
                "CampaignUrlPath",
                "Impressions",
                "Clicks",
                "Cost"
            ],
            "ReportName": "НАЗВАНИЕ_ОТЧЕТА",
            "ReportType": "CUSTOM_REPORT",
            "DateRangeType": "ALL_TIME",
            "Format": "TSV",
            "IncludeVAT": "NO",
            "IncludeDiscount": "NO"
        }
    }

    report = client.reports().post(data=body)
    report().to_dicts()

    df_report = pd.DataFrame(report().to_dicts())

    engine = create_engine(f'sqlite:///{database_file}', connect_args={'check_same_thread': False})
    # insert_new_records(df_report, table_name, unique_cols, database_file)

    df_report.to_sql(table_name, con=engine, if_exists='replace', index=False)
    logging.info("Загрузил данные по Яндекс Директ")

