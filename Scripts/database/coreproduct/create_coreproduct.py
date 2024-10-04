import logging
import sqlite3


def create_coreproduct_metrika_table(database_file):
    try:
        # Подключение к базе данных SQLite
        con = sqlite3.connect(database_file)
        cur = con.cursor()

        # Удаление таблицы coreproduct_metrika, если она существует
        cur.execute("DROP TABLE IF EXISTS coreproduct_metrika")
        # Создание новой таблицы coreproduct_metrika с необходимыми данными
        cur.execute("""
                    CREATE TABLE coreproduct_metrika AS
                     SELECT 
                           date,
                           c.id_campaign,
                           trafficSource,
                           'ig' as UTMSource,
                           b.UTMMedium,
                           b.UTMCampaign,
                           b.UTMContent,
                           startURL,
                           visits,
                           pageviews,
                           users,
                           goal335468328reaches,
                           goal328883051reaches,
                           goal329184866reaches
                    FROM wellbeing b
                    left join corprod_ref_campaigns c ON c.name_campaign = b.UTMCampaign AND c.UTMSource = 'ig' AND 'cpc' = c.UTMMedium
                    WHERE  b.UTMCampaign LIKE '%korprod%' OR b.UTMCampaign LIKE '%Coreprod%'
              
              
        """)
        con.commit()
        con.close()
        logging.info("Таблица coreproduct_metrika успешно создана!")
    except Exception as e:

        logging.error(f"Ошибка при создании таблицы ref_campaigns: {e}")



def create_coreproduct_fb_table(database_file):
    try:
        # Подключение к базе данных SQLite
        con = sqlite3.connect(database_file)
        cur = con.cursor()

        # Удаление таблицы coreproduct_metrika, если она существует
        cur.execute("DROP TABLE IF EXISTS coreproduct_fb")
        # Создание новой таблицы coreproduct_metrika с необходимыми данными
        cur.execute("""
                    CREATE TABLE coreproduct_fb AS
                    SELECT 
                    День,
                   "Название кампании",
                   c.id_campaign,
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
              FROM vygruzka_fb f
              left join corprod_ref_campaigns c ON c.name_campaign = f."Название кампании"
              WHERE f."Название кампании" LIKE '%Coreprod%'
        """)
        con.commit()
        con.close()
        logging.info("Таблица coreproduct_fb успешно создана!")
    except Exception as e:

        logging.error(f"Ошибка при создании таблицы coreproduct_fb: {e}")


def create_coreproduct_bitrix_table(database_file):
    try:
        # Подключение к базе данных SQLite
        con = sqlite3.connect(database_file)
        cur = con.cursor()

        # Удаление таблицы coreproduct_metrika, если она существует
        cur.execute("DROP TABLE IF EXISTS coreproduct_bitrix")
        # Создание новой таблицы coreproduct_metrika с необходимыми данными
        cur.execute("""
                    CREATE TABLE coreproduct_bitrix AS
                       SELECT 
                    b.ID,
                    CASE 
                    
                    WHEN b.UTM_CAMPAIGN='EUR/lead_Coreprod/Site_New (new goal)' THEN 2 
                    WHEN b.UTM_CAMPAIGN='eur/lead_coreprod/site_new (new goal)' THEN 2 
                    ELSE  c.id_campaign
                    END as id_campaign,
                    s.Статус,
                    st.Стадия,
                  
                    NAME,
                    DATE_CREATE,
                    'ig' as UTM_SOURCE,
                    UTM_MEDIUM,
                    UTM_CAMPAIGN,
                    UTM_CONTENT,
                    UTM_TERM,
                    SOURCE_DESCRIPTION
                    FROM bitrix_lead_list b
                    left join corprod_ref_campaigns c ON c.name_campaign = b.UTM_CAMPAIGN AND c.UTMSource = b.UTM_SOURCE AND b.UTM_MEDIUM = c.UTMMedium
                    left join status_leads s ON s.lead_id = b.ID
                      left join stage_bitrix st ON st.ID = b.ID

                    WHERE b.UTM_CAMPAIGN LIKE '%coreprod%'
                    ORDER BY b.DATE_CREATE DESC
        """)
        con.commit()
        con.close()
        logging.info("Таблица coreproduct_bitrix успешно создана!")
    except Exception as e:

        logging.error(f"Ошибка при создании таблицы coreproduct_bitrix: {e}")


def create_corprod_ref_campaigns_table(database_file):
    try:
        # Подключение к базе данных SQLite
        con = sqlite3.connect(database_file)
        cur = con.cursor()

        # Удаление таблицы ref_campaigns, если она существует
        cur.execute("DROP TABLE IF EXISTS corprod_ref_campaigns")

        # Создание новой таблицы ref_campaigns с необходимыми данными
        cur.execute("""
        CREATE TABLE corprod_ref_campaigns (
            id_campaign INTEGER PRIMARY KEY AUTOINCREMENT,
            name_campaign TEXT,
            UTMSource TEXT,
            UTMMedium TEXT
           
            
        )
        """)

        # Вставка данных в таблицу ref_campaigns
        cur.execute("""
        INSERT INTO corprod_ref_campaigns (name_campaign,UTMSource,UTMMedium)
              SELECT
distinct(t.name_campaign),
t.UTMSource,
t.UTMMedium
 FROM ( 


SELECT 
       DISTINCT("Название кампании") AS name_campaign,
       UTMSource,
       UTMMedium
       
  FROM coreproduct_fb
  
UNION ALL 

select 
b.UTMCampaign as name_campaign,
'ig' as UTMSource,
b.UTMMedium
from wellbeing b
 WHERE b.startURL LIKE '%coreproduct/1%' AND b.UTMMedium = 'cpc' AND b.UTMCampaign IN ('EUR/lead_korprod/Site_New/', 'EUR/lead_Coreprod/Site_New (new goal)')
) t
          

        """)

        con.commit()
        con.close()

        logging.info("Таблица corprod_ref_campaigns успешно создана!")
    except Exception as e:
        logging.error(f"Ошибка при создании таблицы corprod_ref_campaigns: {e}")



