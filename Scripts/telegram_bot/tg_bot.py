import asyncio
import os

from aiogram import Bot, Dispatcher, F

from aiogram.types import ContentType

from dotenv import load_dotenv

from telegram_bot.handlers import routers


load_dotenv()


async def main():
    bot = Bot(token=os.getenv("BOT_TOKEN"))
    dp = Dispatcher()
    for router in routers:
        dp.include_router(router)
    try:
        await dp.start_polling(bot)


    except KeyboardInterrupt:
        print("Bot Stopped")


if __name__=='__main__':
    asyncio.run(main())
