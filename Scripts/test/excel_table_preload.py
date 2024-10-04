import pandas as pd

# Загрузить Excel файл
file_path = 'C:\\Users\\vladimir\PycharmProjects\\001_Nalsed\data\\file.xlsx'
df = pd.read_excel(file_path)


# Удалить строки, где в столбце 'Название кампании' пусто
df_cleaned = df.dropna(subset=['Название кампании'])

# Функция для разделения строки URL параметров и создания новых столбцов
def split_url_params(row):
    if pd.isna(row):
        return {}
    params = row.split('&')
    param_dict = {}
    for param in params:
        key, value = param.split('=')
        param_dict[key] = value
    return param_dict

# Применить функцию к столбцу 'Параметры URL' и создать новые столбцы
url_params = df_cleaned['Параметры URL'].apply(split_url_params)
url_params_df = pd.json_normalize(url_params)

# Объединить очищенный DataFrame с новыми столбцами
df_expanded = pd.concat([df_cleaned, url_params_df], axis=1)

df_expanded['utm_term'] = ''

# Определить порядок столбцов
# Предполагая, что вы знаете все новые столбцы, добавленные из URL параметров:
new_url_columns = list(url_params_df.columns)

# Порядок столбцов, как на скриншоте, включая новые столбцы из URL параметров
new_columns_order = [
    'День',
    'Название кампании',
    'utm_campaign',  # Замените это на фактическое название столбца, если оно отличается
    'Название группы объявлений',
    'utm_content',  # Замените это на фактическое название столбца, если оно отличается
    'Название объявления',
    'utm_term',  # Замените это на фактическое название столбца, если оно отличается
    'Статус показа',
    'Уровень показа',
    'Тип результата',
    'Результат',
    'Охват',
    'Показы',
    'Цена за результат',
    'Клики по ссылке',
    'Просмотры целевой страницы'
] + [col for col in new_url_columns if col not in [
    'utm_campaign', 'utm_content', 'utm_term']]

# Переупорядочить столбцы
df_expanded = df_expanded[new_columns_order]

# Сохранить расширенный DataFrame в новый Excel файл
output_file_path = 'file_2.xlsx'
df_expanded.to_excel(output_file_path, index=False)

print(f'Расширенный файл сохранен как {output_file_path}')