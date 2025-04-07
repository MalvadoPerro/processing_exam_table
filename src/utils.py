# Standard Libraries
import os
import re
import time
from collections import Counter

# Third Party Libraries
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

# Local Libraries
import src.constants as const


def auth():
    """Авторизация"""
    print("Начата авторизация.")
    creds = None
    # Файл token.json хранит данные авторизованного пользователя, 
    # он создается автоматически после авторизации при первом запуске
    if os.path.exists(const.PATH_TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(const.PATH_TOKEN_FILE, const.SCOPES)
    # Если его нет, нужно авторизоваться
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                const.PATH_CREDS_FILE, const.SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Сохраняем параметры для последующего использования
        with open(const.PATH_TOKEN_FILE, "w") as token:
            token.write(creds.to_json())
    print("Авторизация прошла успешно.")
    return gspread.authorize(creds)


def get_source_data(client: gspread.Client):
    """Получение данных из листа 'Оценки'"""
    print("Получение данных.")
    attempt = 0
    max_attempts = 3
    
    while attempt < max_attempts:
        attempt += 1
        try:
            print(f"Попытка {attempt}/{max_attempts}")
            
            # Проверка клиента
            if not hasattr(client, 'open_by_key'):
                raise ValueError("Некорректный клиент Google Sheets.")
            
            # Проверка ID таблицы
            if not const.SPREADSHEET_ID or len(const.SPREADSHEET_ID) != 44:
                raise ValueError("Некорректный формат ID таблицы.")
            
            print(f"Открытие таблицы: {const.SPREADSHEET_ID}.")
            spreadsheet = client.open_by_key(const.SPREADSHEET_ID)

            print(f"Поиск листа: {const.SOURCE_SHEET_NAME}.")
            worksheet = spreadsheet.worksheet(const.SOURCE_SHEET_NAME)
            
            headers = worksheet.row_values(1)
            duplicates = [item for item, count in Counter(headers).items() if count > 1]
            if duplicates:
                print(f"Найдены дубликаты: {duplicates}. \nИх необходимо почистить.")
            
            print("Чтение данных...")
            records = worksheet.get_all_records()
            
            if not records:
                print("Получены пустые данные.")
                return pd.DataFrame()
            
            df = pd.DataFrame(records)
            print(f"Успешно получено {len(df)} строк.")
            return df
            
        except Exception as e:
            print(f"Ошибка: {str(e)}")
            if attempt == max_attempts:
                raise
            print(f"Повтор через 5 секунд...")
            time.sleep(5)
    
    raise Exception("Не удалось получить данные после нескольких попыток.")


def processing_data(df: pd.DataFrame):
    """Обработка данных и разделение на подгруппы"""
    print("Начата обработка данных.")
    if not all(col in df.columns for col in const.BASE_COLS):
        raise ValueError("Отсутствуют базовые колонки в данных.")

    result = {}
    for section_num in range(2, 8):
        pattern = rf'^{re.escape(str(section_num))}\..+'

        section_cols = [
            col for col in df.columns
            if re.match(pattern, col) and col not in const.BASE_COLS
        ]
        
        if not section_cols:
            print(f"Предупреждение: Нет колонок для раздела {section_num}.")
            continue

        section_cols = sorted(
            section_cols,
            key=lambda x: int(x.split('.')[1]) if x.split('.')[1].isdigit() else 0
        )

        result[section_num] = df[const.BASE_COLS + section_cols]
        print(f"Записал вопросы раздела {section_num}.")

    return result


def write_results(client: gspread.Client, processed_data: dict):
    """Запись результатов на отдельные листы"""
    print("Начата запись результатов на отдельные листы.")
    spreadsheet = client.open_by_key(const.SPREADSHEET_ID)
    
    for section_num, df in processed_data.items():
        sheet_name = f"Раздел {section_num}"
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(
                title=sheet_name, 
                rows=max(df.shape[0], 100), 
                cols=max(df.shape[1], 10)
            )
        
        worksheet.clear()
        set_with_dataframe(worksheet, df)
        print(f"Данные для раздела {section_num} записаны.")
