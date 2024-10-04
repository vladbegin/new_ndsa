from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

# Правильно определённая inline клавиатура
report_keyboard = InlineKeyboardMarkup(inline_keyboard=[
    [
        InlineKeyboardButton(text='Coreproduct/4', callback_data='get_coreproduct_4'),
        InlineKeyboardButton(text='Coreproduct/5', callback_data='get_coreproduct_5')
    ]
    # ,
    # [
    #     InlineKeyboardButton(text='Coreproduct', callback_data='get_coreproduct')
    # ]
])
