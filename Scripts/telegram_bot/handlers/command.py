from aiogram import F, Router, types
from aiogram.filters import Command, CommandStart

from Scripts.telegram_bot.keyboards.inline_keyboard import report_keyboard

router = Router()

@router.message(CommandStart())
async def cmd_start(message: types.Message):
    welcome_message = (f'''Привет! Я бот который присылает статистику по расходу и записям на регистрацию''')
    await message.answer(welcome_message, reply_markup=report_keyboard) #reply_markup=kb.main


@router.message(Command('help'))
async def cmd_help(message: types.Message):
    await message.answer(
        text="Я бот который присылает статистику по расходу и записям на регистрацию"

    ) #reply_markup=ikb.instructions

@router.message(F.text =='/get_report')
async def get_stat_command(message: types.Message):
    await message.answer(
        text='Выберите проект',
        reply_markup= report_keyboard

    )



