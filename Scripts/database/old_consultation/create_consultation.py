import logging
import sqlite3

def create_consultation_metrika_table(database_file):
    try:
        # Подключение к базе данных SQLite
        con = sqlite3.connect(database_file)
        cur = con.cursor()

        # Удаление таблицы consultation_metrika, если она существует
        cur.execute("DROP TABLE IF EXISTS consultation_metrika")

        # Создание новой таблицы consultation_metrika с необходимыми данными
        cur.execute(
            """
            CREATE TABLE consultation_metrika AS
            SELECT date,
           trafficSource,
           r.id_campaign,
           c.UTMSource,
           c.UTMMedium,
           c.UTMCampaign,
           startURL,
           visits,
           pageviews,
           users
              FROM consultation c
              LEFT JOIN consultation_ref_campaigns r ON r.UTMCampaign = c.UTMCampaign AND c.UTMSource = r.UTMSource AND c.UTMMedium = r.UTMMedium
              where c.startURL LIKE '%/free-consultation%';
                        
            """
        )


    except Exception as e:
        logging.error(f"Ошибка при создании таблицы consultation_metrika: {e}")

def create_consultation_ref_campaigns_table(database_file):
    try:
        # Подключение к базе данных SQLite
        con = sqlite3.connect(database_file)
        cur = con.cursor()

        # Удаление таблицы consultation_ref_campaigns, если она существует
        cur.execute("DROP TABLE IF EXISTS consultation_ref_campaigns")

        # Создание новой таблицы consultation_ref_campaigns с необходимыми данными
        cur.execute("""
                CREATE TABLE consultation_ref_campaigns (
                    id_campaign INTEGER PRIMARY KEY AUTOINCREMENT,
                    UTMCampaign TEXT,
                    UTMSource TEXT,
                    UTMMedium TEXT
                )
                """)

        # Вставка данных в таблицу consultation_ref_campaigns
        cur.execute(
            '''
            INSERT INTO consultation_ref_campaigns (UTMCampaign,UTMSource,UTMMedium)
              SELECT 
DISTINCT(t.UTMCampaign) AS UTMCampaign,
t.UTMSource,
t.UTMMedium 


FROM (




SELECT UTMCampaign AS UTMCampaign,
            UTMSource,
           UTMMedium     
            FROM consultation c
            where c.startURL LIKE '%/free-consultation%' AND c.UTMSource != 'test' AND c.UTMCampaign !=''
            

UNION ALL 

SELECT 
UTM_campaign AS UTMCampaign,       
'ig' AS UTMSource,
       'cpc' UTMMedium
  FROM vygruzka_fb f
  WHERE f.UTM_campaign LIKE '%consult%' ) t
            
            '''
        )
    except Exception as e:
        logging.error(f"Ошибка при создании таблицы consultation_ref_campaigns: {e}")



def create_consultation_fb_table(database_file):
    try:
        # Подключение к базе данных SQLite
        con = sqlite3.connect(database_file)
        cur = con.cursor()

        # Удаление таблицы consultation_fb_table, если она существует
        cur.execute("DROP TABLE IF EXISTS consultation_fb_table")
        # Создание новой таблицы consultation_fb_table с необходимыми данными
        cur.execute("""
                    CREATE TABLE consultation_fb_table AS
                    WITH FB as (
SELECT 
                    День,
                   "Название кампании",
                  
                   'ig' as UTMSource,
                   'cpc' as UTMMedium,
                   UTM_campaign,
                   "Название группы объявлений",
                   UTM_content,
                   "Название объявления",
                   UTM_term,
                   Результат,
                   Охват,
                   Показы,
                   "Цена за результат",
                   "Клики по ссылке",
                   "Просмотры целевой страницы",
                   "Сумма, руб."
              FROM vygruzka_fb f) 
SELECT 
c.id_campaign,
 День,
                   "Название кампании",
                   f.UTMMedium,
                   f.UTM_campaign,
                   "Название группы объявлений",
                   f.UTM_content,
                   "Название объявления",
                   f.UTM_term,
                   Результат,
                   Охват,
                   Показы,
                   "Цена за результат",
                   "Клики по ссылке",
                   "Просмотры целевой страницы",
                   "Сумма, руб."
FROM FB f
left join consultation_ref_campaigns c ON c.UTMCampaign = f.UTM_campaign AND c.UTMSource = f.UTMSource AND c.UTMMedium = f.UTMMedium
                  WHERE f."UTM_campaign" LIKE '%consul%';
        """)
        con.commit()
        con.close()
        logging.info("Таблица consultation_fb_table успешно создана!")
    except Exception as e:

        logging.error(f"Ошибка при создании таблицы consultation_fb_table: {e}")


def create_consultation_bitrix_table(database_file):
    try:
        # Подключение к базе данных SQLite
        con = sqlite3.connect(database_file)
        cur = con.cursor()

        # Удаление таблицы coreproduct_metrika, если она существует
        cur.execute("DROP TABLE IF EXISTS consultation_bitrix_table")
        # Создание новой таблицы consultation_bitrix_table с необходимыми данными
        cur.execute("""
                    CREATE TABLE consultation_bitrix_table AS
with bitrix as (SELECT 
                    ID,
                    s.Стадия,
                    NAME,
                    DATE_CREATE,
                    'ig' as UTM_SOURCE,
                    UTM_MEDIUM,
                    UTM_CAMPAIGN,
                    UTM_CONTENT,
                    UTM_TERM,
                    SOURCE_DESCRIPTION
                    FROM bitrix_lead_list b
                    left join stage_bitrix s ON s.ID = b.ID
                    WHERE b.UTM_CAMPAIGN LIKE '%consul%') 
                    
select 
c.id_campaign,
b.*
from bitrix b
left join consultation_ref_campaigns c ON c.UTMCampaign = b.UTM_CAMPAIGN AND c.UTMSource = b.UTM_SOURCE AND b.UTM_MEDIUM = c.UTMMedium
        """)
        con.commit()
        con.close()
        logging.info("Таблица consultation_bitrix успешно создана!")
    except Exception as e:

        logging.error(f"Ошибка при создании таблицы consultation_bitrix: {e}")