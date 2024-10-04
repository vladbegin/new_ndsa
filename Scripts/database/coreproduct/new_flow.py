import logging
import sqlite3

def create_ref_source(database_file):
    try:
        # Подключение к базе данных SQLite
        con = sqlite3.connect(database_file)
        cur = con.cursor()

        # Удаление таблицы, если она существует
        cur.execute("DROP TABLE IF EXISTS ref_source")

        # Создание новой таблицы
        cur.execute("""CREATE TABLE IF NOT EXISTS ref_utm_source (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        utm_source TEXT UNIQUE""")
        con.commit()
        con.close()
        logging.info("Таблица ref_source успешно создана!")

    except Exception as ex:
        logging.error(f"Ошибка при создании таблицы ref_source: {ex}")


def create_ref_campaigns(database_file):
    try:
        # Подключение к базе данных SQLite
        con = sqlite3.connect(database_file)
        cur = con.cursor()

        # Удаление таблицы, если она существует
        cur.execute("DROP TABLE IF EXISTS ref_campaigns")
        # Создание новой таблицы
        cur.execute("""CREATE TABLE IF NOT EXISTS ref_campaigns (
                        CampaignId INTEGER PRIMARY KEY,
                        CampaignName TEXT,
                        product TEXT,
                        utm_source_id INTEGER,
                        FOREIGN KEY (utm_source_id) REFERENCES ref_source(id)
)""")
        con.commit()
        con.close()
        logging.info("Таблица ref_campaigns успешно создана!")

    except Exception as ex:
        logging.error(f"Ошибка при создании таблицы ref_campaigns: {ex}")


def insert_ref_campaign(database_file):
    try:
        # Подключение к базе данных SQLite
        con = sqlite3.connect(database_file)
        cur = con.cursor()

        # Удаление таблицы, если она существует
        cur.execute("DROP TABLE IF EXISTS ref_campaigns")
        # Создание новой таблицы
        cur.execute("""INSERT INTO ref_campaigns (CampaignId, CampaignName, product, utm_source_id)
                        SELECT DISTINCT t.CampaignId,
                               t.CampaignName,
                               CASE 
                                   WHEN t.CampaignUrlPath LIKE '%coreproduct/4%' THEN 'coreproduct/4'
                                   WHEN t.CampaignUrlPath LIKE '%coreproduct/5%' THEN 'coreproduct/5'
                                   WHEN t.CampaignUrlPath LIKE '%wellbeing%' THEN 'wellbeing'
                                   WHEN t.CampaignUrlPath LIKE '%fairy%' THEN 'storyteller'
                                   WHEN t.CampaignUrlPath LIKE '%coreproduct/6%' THEN 'coreproduct/6'
                                   ELSE 'unknown'
                               END as product,
                               u.id -- Связь с таблицей utm_source
                        FROM yandex_direct t
                        LEFT JOIN ref_utm_source u ON t.UTMSource = u.UTMSource
    )""")
        con.commit()
        con.close()
        logging.info("Таблица ref_campaigns успешно создана!")

    except Exception as ex:
        logging.error(f"Ошибка при создании таблицы ref_campaigns: {ex}")
