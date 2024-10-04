import logging
import sqlite3

def create_coreproduct5_metrika_table(database_file):
    try:
        # Подключение к базе данных SQLite
        con = sqlite3.connect(database_file)
        cur = con.cursor()

        # Удаление таблицы coreproduct_metrika, если она существует
        cur.execute("DROP TABLE IF EXISTS coreproduct5_metrika")

        # Создание новой таблицы coreproduct_metrika с необходимыми данными
        cur.execute("""CREATE TABLE coreproduct5_metrika AS
         SELECT date,
                       trafficSource,
                       UTMSource,
                       UTMMedium,
                       UTMCampaign,
                       UTMContent,
                       startURL,
                       visits,
                       pageviews,
                       users,
                       goal335468328reaches,
                       goal328883051reaches,
                       goal329184866reaches
                  FROM wellbeing w
                  WHERE w.startURL LIKE '%coreproduct/5%'
            """)
        con.commit()
        con.close()
        logging.info("Таблица coreproduct5_metrika_table успешно создана!")
    except Exception as ex:
        logging.error(f"Ошибка при создании таблицы coreproduct4_metrika_table: {ex}")



def create_coreproduct5_bitrix_table(database_file):
    try:
        # Подключение к базе данных SQLite
        con = sqlite3.connect(database_file)
        cur = con.cursor()

        # Удаление таблицы coreproduct_metrika, если она существует
        cur.execute("DROP TABLE IF EXISTS coreproduct5_bitrix")
        # Создание новой таблицы coreproduct_metrika с необходимыми данными
        cur.execute("""
                    CREATE TABLE coreproduct5_bitrix AS
                  SELECT b.ID,
        s.Стадия,
       DATE_CREATE,
       UTM_SOURCE,
       UTM_MEDIUM,
       UTM_CONTENT,
       UTM_CAMPAIGN,
       UTM_TERM,
       SOURCE_DESCRIPTION,
       TITLE,
       CREATED_BY_ID,
       LAST_NAME,
       NAME
  FROM bitrix_lead_list b
  left join stage_bitrix s ON s.ID = b.ID
  WHERE b.SOURCE_DESCRIPTION LIKE '%coreproduct/5%'
        """)
        con.commit()
        con.close()
        logging.info("Таблица coreproduct5_bitrix успешно создана!")
    except Exception as e:

        logging.error(f"Ошибка при создании таблицы coreproduct5_bitrix: {e}")