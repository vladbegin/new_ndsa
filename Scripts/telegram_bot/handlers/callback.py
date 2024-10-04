import os
import sqlite3
import asyncio
from aiogram import F, Router, types
from aiogram.filters import CommandStart
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Относительный путь в переменной окружения .env
db_name = os.getenv('db_name')

# Путь к корневой директории проекта
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))

# Полный путь к базе данных
db_path = os.path.join(project_root, db_name)

router = Router()

# Обработка callback_query с данными 'get_coreproduct'
@router.callback_query(F.data == 'get_coreproduct_4')
async def calculate_data(callback_query: types.CallbackQuery):
    # Начало выполнения задачи
    progress_message = await callback_query.message.answer("Начал подсчет данных...")

    # Подключение к базе данных
    try:
        # Update the progress (step 1)
        await progress_message.edit_text("⏳ Подключение к базе данных...")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Update the progress (step 2)
        await asyncio.sleep(1)  # Simulate some waiting time
        await progress_message.edit_text("⏳ Выполняю запрос к базе данных...")

        # Выполнение SQL-запроса
        query = f"""WITH bitrix AS (
                            SELECT 
                                COUNT(DISTINCT(ID)) AS leads,
                                DATE(DATE_CREATE) AS Date
                            FROM 
                                coreproduct4_bitrix c
                            WHERE 
                                c.SOURCE_DESCRIPTION LIKE '%coreproduct/4%' 
                                AND c.UTM_SOURCE = 'yandex'
                            GROUP BY 
                                DATE(DATE_CREATE)
                        ),
                        yd AS (
                            SELECT 
                                d.Date,   
                                SUM(CAST(d.Cost AS REAL)) AS cost
                            FROM 
                                yandex_direct d
                            WHERE 
                                d.CampaignUrlPath LIKE '%coreproduct/4%'
                                AND d.Date = DATE('now', '-1 day')
                            GROUP BY 
                                d.Date
                        ),
                        
                        users as ( Select 
                                    m.date as Date,
                                    SUM(m.users) as users
                                    
                                    FROM coreproduct4_metrika m
                                    where m.startURL LIKE '%coreproduct/4%'
                                    GROUP BY  m.date
)
                        


                        SELECT 
                            yd.Date,
                            ROUND(yd.cost,2) as cost,
                            bitrix.leads,
                            ROUND((bitrix.leads / users.users) * 100 ) || '%' AS CR1,
                           ROUND(yd.cost / bitrix.leads,2) as cpl
                        FROM 
                            yd
                        LEFT JOIN bitrix ON yd.Date = bitrix.Date
                        LEFT JOIN users ON yd.Date = users.Date
                        ORDER BY 
                            yd.Date DESC
                """
        cursor.execute(query)

        # Update the progress (step 3)
        await asyncio.sleep(1)  # Simulate another waiting time
        await progress_message.edit_text("⏳ Обработка данных...")

        # Получение результата
        result = cursor.fetchone()
        if result:
            date = result[0]
            cost = result[1]
            leads = result[2]
            cr = result[3]
            cpl = result[4]

            await progress_message.edit_text(f"✅ Подсчет завершен")
            await progress_message.edit_text(
                f"✅ **Подсчет завершен**\n"
                f"*Лендинг coreproduct/4*\n\n"
                f"*Дата*: `{date}`\n"
                f"*Расход*: `{cost}` руб.\n"
                f"*Конверсия в регистрацию*: `{cr}`\n"
                f"*Регистрации*: `{leads}`\n"
                f"*Цена за регистрацию*: `{cpl}` руб.",
                parse_mode="Markdown"
            )
        else:
            await progress_message.edit_text("❌ Лиды не найдены.")

    except sqlite3.OperationalError as e:
        # Handle database connection errors
        print(f"Ошибка подключения: {e}")
        await progress_message.edit_text(f"Ошибка подключения: {e}")
    finally:
        # Always ensure to close the database connection
        conn.close()


# Обработка callback_query с данными 'get_coreproduct'
@router.callback_query(F.data == 'get_coreproduct_5')
async def calculate_data(callback_query: types.CallbackQuery):
    # Начало выполнения задачи
    progress_message = await callback_query.message.answer("Начал подсчет данных...")

    # Подключение к базе данных
    try:
        # Update the progress (step 1)
        await progress_message.edit_text("⏳ Подключение к базе данных...")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Update the progress (step 2)
        await asyncio.sleep(1)  # Simulate some waiting time
        await progress_message.edit_text("⏳ Выполняю запрос к базе данных...")

        # Выполнение SQL-запроса
        query = f"""WITH bitrix AS (
                            SELECT 
                                COUNT(DISTINCT(ID)) AS leads,
                                DATE(DATE_CREATE) AS Date
                            FROM 
                                coreproduct5_bitrix c
                            WHERE 
                                c.SOURCE_DESCRIPTION LIKE '%coreproduct/5%' 
                                AND c.UTM_SOURCE = 'yandex'
                            GROUP BY 
                                DATE(DATE_CREATE)
                        ),
                        yd AS (
                            SELECT 
                                d.Date,   
                                SUM(CAST(d.Cost AS REAL)) AS cost
                            FROM 
                                yandex_direct d
                            WHERE 
                                d.CampaignUrlPath LIKE '%coreproduct/5%'
                                AND d.Date = DATE('now', '-1 day')
                            GROUP BY 
                                d.Date
                        ),
                        
                        users as ( Select 
                                    m.date as Date,
                                    SUM(m.users) as users
                                    
                                    FROM coreproduct5_metrika m
                                    where m.startURL LIKE '%coreproduct/5%'
                                    GROUP BY  m.date
)
                        


                        SELECT 
                            yd.Date,
                            ROUND(yd.cost,2) as cost,
                            bitrix.leads,
                            ROUND((bitrix.leads / users.users) * 100 ) || '%' AS CR1,
                           ROUND(yd.cost / bitrix.leads,2) as cpl
                        FROM 
                            yd
                        LEFT JOIN bitrix ON yd.Date = bitrix.Date
                        LEFT JOIN users ON yd.Date = users.Date
                        ORDER BY 
                            yd.Date DESC
                """
        cursor.execute(query)

        # Update the progress (step 3)
        await asyncio.sleep(1)  # Simulate another waiting time
        await progress_message.edit_text("⏳ Обработка данных...")

        # Получение результата
        result = cursor.fetchone()
        if result:
            date = result[0]
            cost = result[1]
            leads = result[2]
            cr = result[3]
            cpl = result[4]

            await progress_message.edit_text(f"✅ Подсчет завершен")
            await progress_message.edit_text(
                f"✅ **Подсчет завершен**\n"
                f"*Лендинг coreproduct/4*\n\n"
                f"*Дата*: `{date}`\n"
                f"*Расход*: `{cost}` руб.\n"
                f"*Конверсия в регистрацию*: `{cr}`\n"
                f"*Регистрации*: `{leads}`\n"
                f"*Цена за регистрацию*: `{cpl}` руб.",
                parse_mode="Markdown"
            )
        else:
            await progress_message.edit_text("❌ Лиды не найдены.")

    except sqlite3.OperationalError as e:
        # Handle database connection errors
        print(f"Ошибка подключения: {e}")
        await progress_message.edit_text(f"Ошибка подключения: {e}")
    finally:
        # Always ensure to close the database connection
        conn.close()

