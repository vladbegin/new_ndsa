# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import datetime

from aiogram import Router
# from dateutil import parser
import logging
import os
from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz  # для установки московской часовой зоны
from Scripts.api.bitrix import fetch_and_store_bitrix_leads
from Scripts.api.google_sheets import load_data_from_google_sheets, get_status_lead_from_google_sheet, credentials_file
from Scripts.api.yandex_direct import get_data_from_yandex_direct
from Scripts.api.yandex_metrika import fetch_and_store_data
from Scripts.database.coreproduct.create_coreproduct import create_coreproduct_metrika_table, \
    create_coreproduct_fb_table, create_coreproduct_bitrix_table, create_corprod_ref_campaigns_table
from Scripts.database.coreproduct4.proccess import create_coreproduct4_metrika_table, create_coreproduct4_bitrix_table, \
    create_content_bitrix_table
from Scripts.database.coreproduct5.proccess5 import create_coreproduct5_metrika_table, create_coreproduct5_bitrix_table
from Scripts.database.create_database_f import create_db, get_max_date_from_table
from Scripts.database.old_consultation.create_consultation import create_consultation_metrika_table, \
    create_consultation_ref_campaigns_table, create_consultation_fb_table, create_consultation_bitrix_table
from Scripts.test.test import fetch_and_store_bitrix_leads_test, bitrix_leads_stage_preload



def scheduled_task():
    logging.info("Запуск задачи по расписанию...")

    main(
        start_date='2024-05-01',
        database_name=os.getenv('db_name'),
        bitrix=1,
        fb=1,
        vk=1,
        coreproduct=1,
        consultation=1,
        fairy=1,
        yandex_direct=1
    )



def main(database_name,start_date,bitrix,fb,coreproduct,consultation, fairy, vk, yandex_direct):
    credentials_file = 'config/nimble-hash-276219-1c965d6cdc2e.json'
    spreadsheet_url = "https://docs.google.com/spreadsheets/d/1MDxmTeL1mnfk-rtBxXT1-QOL5Ovixp4FFCragCYEK98/edit?gid=893463152#gid=893463152"
    worksheet_name = 'Календарь ПШ'
    create_db(database_name)
    bitrix_leads_stage_preload(database_file=database_name, table_name='stage_bitrix')
    get_status_lead_from_google_sheet(credentials_file=credentials_file, spreadsheet_url=spreadsheet_url, worksheet_name=worksheet_name, table_name='status_leads', database_file=os.getenv('db_name'))
    if bitrix == 1:
        '''Получаю заявки из битрикс'''
        table_name='bitrix_lead_list'
        # fetch_and_store_bitrix_leads(db_path=database_name, start_date=start_date)
        start_date = get_max_date_from_table(table_name=table_name, column_name='DATE_CREATE', database_file=database_name)
        # start_date = datetime.strptime(start_date, '%Y-%m-%d').date()

        logging.info(f"get date from database from {table_name}. Start date {start_date}")
        unique_cols = ['ID']
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        fetch_and_store_bitrix_leads_test(database_file=database_name, start_date=start_date, table_name=table_name, unique_cols=unique_cols)

        create_coreproduct_bitrix_table(database_file=database_name)
        create_coreproduct4_bitrix_table(database_file=database_name)
        create_coreproduct5_bitrix_table(database_file=database_name)


    if vk == 1:
        '''Загрузка из ВК'''


        spreadsheet_url = os.getenv('spreadsheet_url')
        worksheet_name = 'Выгрузка VK'
        expected_headers = [
            'Дата', 'ID кампании', 'ID группы', 'ID объявления', 'Название объявления',
            'Показы', 'Клики', 'Результат', 'Цена за результат, ₽', 'Потрачено, ₽'
        ]
        numeric_columns = ['Потрачено, ₽', 'Цена за результат, ₽']
        table_name = 'vygruzka_vk'
        load_data_from_google_sheets(credentials_file, spreadsheet_url, worksheet_name, expected_headers,
                                     numeric_columns,
                                     table_name, database_file=database_name)

    if fb == 1:
        '''Получаю данные с таблицы ФБ'''
        credentials_file = 'config/nimble-hash-276219-1c965d6cdc2e.json'
        spreadsheet_url = os.getenv('spreadsheet_url')
        worksheet_name = 'Выгрузка FB'
        expected_headers = [
            'День', 'Название кампании', 'UTM_campaign', 'Название группы объявлений',
            'UTM_content', 'Название объявления', 'UTM_term', 'Результат', 'Охват',
            'Показы', 'Цена за результат', 'Клики по ссылке', 'Просмотры целевой страницы',
            'Сумма, руб.'
        ]
        table_name = 'vygruzka_fb'
        numeric_columns = ['Сумма, руб.']

        load_data_from_google_sheets(credentials_file, spreadsheet_url, worksheet_name, expected_headers,
                                     numeric_columns,
                                     table_name,database_file=database_name)

        #Из списка рекламных кампаний в ФБ создаю справочник кампаний
        create_corprod_ref_campaigns_table(database_file=database_name)
        create_coreproduct_fb_table(database_file=database_name)
    #Получаю сырые данные с яндекс метрики для корпродукт
    if coreproduct == 1:
        logging.info(f"get data from yandex metrika for благо   ")
        counter_id = os.getenv('ym_counter_landing_nasledie_digital')
        metrics = 'ym:s:visits,ym:s:pageviews,ym:s:users,ym:s:goal335468328reaches,  ym:s:goal328883051reaches, ym:s:goal329184866reaches'
        dimensions = 'ym:s:date,ym:s:trafficSource,ym:s:UTMSource,ym:s:UTMMedium,ym:s:UTMCampaign,ym:s:UTMContent,ym:s:startURL'
        table_name = 'wellbeing'
        # start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date = datetime.now().date() - timedelta(days=1)
        start_date = get_max_date_from_table(table_name=table_name, column_name='date',database_file=database_name)
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        # print(f"start_date for coreproduct {start_date}")
        logging.info(f"get date from database from {table_name}. Start date {start_date}")
        # end_date = datetime.now().date() #- timedelta(days=1)
        unique_cols = ['date', 'trafficSource', 'UTMSource', 'UTMMedium', 'UTMCampaign', 'startURL', 'visits',
                       'pageviews', 'users']
        fetch_and_store_data(start_date, end_date, counter_id, metrics, dimensions, table_name, unique_cols,
                             database_file=os.getenv('db_name'))
        #
        # #Создаю отфильтрованную таблицу и с JOIn corprod_ref_campaigns
        create_coreproduct_metrika_table(database_file=database_name)
        create_coreproduct4_metrika_table(database_file=database_name)
        create_coreproduct5_metrika_table(database_file=database_name)
        create_content_bitrix_table(database_file=database_name)





    if consultation == 1:
        counter_id = os.getenv('ym_counter_nasledie_digital_rf')
        metrics = 'ym:s:visits,ym:s:pageviews,ym:s:users'
        dimensions = 'ym:s:date,ym:s:trafficSource,ym:s:UTMSource,ym:s:UTMMedium,ym:s:UTMCampaign,ym:s:startURL'
        table_name = 'consultation'
        start_date = get_max_date_from_table(table_name=table_name, column_name='date',database_file=database_name)
        logging.info(f"get date from database from {table_name}. Start date {start_date}")
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        # start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date = datetime.now().date() #- timedelta(days=1)
        unique_cols = ['date', 'trafficSource', 'UTMSource', 'UTMMedium', 'UTMCampaign', 'startURL', 'visits',
                       'pageviews',
                       'users']

        fetch_and_store_data(start_date, end_date, counter_id, metrics, dimensions, table_name, unique_cols,
                             database_file=database_name)
        create_consultation_ref_campaigns_table(database_file=database_name)
        create_consultation_metrika_table(database_file=database_name)
        create_consultation_fb_table(database_file=database_name)
        create_consultation_bitrix_table(database_file=database_name)

    if fairy == 1:
        counter_id = os.getenv('ym_counter_nasledie_digital')
        metrics = 'ym:s:visits,ym:s:pageviews,ym:s:users,ym:s:goal340576407reaches, ym:s:goal334217579reaches'
        dimensions = 'ym:s:date,ym:s:trafficSource,ym:s:startURL,ym:s:UTMSource,ym:s:UTMMedium,ym:s:UTMCampaign,ym:s:UTMContent,ym:s:UTMTerm '
        table_name = 'fairy'
        start_date = get_max_date_from_table(table_name=table_name, column_name='date', database_file=database_name)
        logging.info(f"get date from database from {table_name}. Start date {start_date}")
        # start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()

        end_date = datetime.now().date() #- timedelta(days=1)
        unique_cols = ['date', 'trafficSource', 'UTMSource', 'UTMMedium', 'UTMCampaign', 'startURL', 'visits',
                       'pageviews',
                       'users']

        fetch_and_store_data(start_date, end_date, counter_id, metrics, dimensions, table_name, unique_cols,
                             database_file=database_name)

        '''Параметры для сказочника 2 '''
        logging.info(f"get data from yandex metrika for сказочник   ")
        counter_id = os.getenv('ym_counter_new_capsule')
        metrics = 'ym:s:visits,ym:s:pageviews,ym:s:users,ym:s:goal334026719reaches,ym:s:goal334027507reaches,ym:s:goal334027514reaches,ym:s:goal334027515reaches,ym:s:goal334027516reaches'
        dimensions = 'ym:s:date,ym:s:trafficSource,ym:s:UTMSource,ym:s:UTMMedium,ym:s:UTMCampaign,ym:s:UTMContent,ym:s:UTMTerm,ym:s:startURL'
        table_name = 'fairy_tale'
        start_date = get_max_date_from_table(table_name=table_name, column_name='date', database_file=database_name)
        # start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        # logging.info(f"get date from database from {table_name}. Start date {start_date}")
        # if isinstance(start_date, str):
        #     start_date = datetime.strptime(start_date, '%Y-%m-%d').date()

        end_date = datetime.now().date() # - timedelta(days=1)

        unique_cols = ['date', 'trafficSource', 'UTMSource', 'UTMMedium', 'UTMCampaign', 'startURL', 'visits',
                       'pageviews',
                       'users']

        fetch_and_store_data(start_date, end_date, counter_id, metrics, dimensions, table_name, unique_cols,
                             database_file=os.getenv('db_name'))


    if yandex_direct == 1:
        table_name='yandex_direct'
        unique_cols = ['Date','CampaignId','AdGroupId','AdId']
        get_data_from_yandex_direct(table_name,unique_cols=unique_cols, database_file=database_name)





# Press the gre
# en button in the gutter to run the script.
if __name__ == '__main__':
    main(
        start_date='2024-05-01',
        database_name=os.getenv('db_name'),
        bitrix=1,
        fb=1,
        vk=1,
        coreproduct=1,
        consultation=1,
        fairy=1,
        yandex_direct=1
    )
    # # Инициализация планировщика
    # scheduler = BlockingScheduler(timezone=pytz.timezone('Europe/Moscow'))  # Устанавливаем Московскую зону
    #
    # # Добавление задачи в планировщик: каждый день в 8 утра по московскому времени
    # scheduler.add_job(scheduled_task, CronTrigger(hour=7, minute=0))
    #
    # logging.info("Запуск планировщика...")
    #
    # try:
    #     # Запуск планировщика
    #     scheduler.start()
    # except (KeyboardInterrupt, SystemExit):
    #     logging.info("Завершение работы планировщика.")

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
