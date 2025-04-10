# Standard Libraries
import os
import time
from collections import Counter
import json

# Third Party Libraries
import pandas as pd
import numpy as np
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
                raise ValueError
            
            print("Чтение данных...")
            records = worksheet.get_all_records()
            
            if not records:
                print("Получены пустые данные.")
                return pd.DataFrame()
            
            df = pd.DataFrame(records)
            print(f"Успешно получено {len(df)} строк.")
            return df
            
        except ValueError as e:
            break
        except Exception as e:
            print(f"Ошибка: {str(e)}")
            if attempt == max_attempts:
                raise
            print(f"Повтор через 5 секунд...")
            time.sleep(5)
    
    raise Exception("Не удалось получить данные.")


def preprocessing_data(df: pd.DataFrame):
    """Обработка данных, вычисление кол-ва правильных ответов"""
    print("Начата первичная обработка данных.")
    if not all(col in df.columns for col in const.BASE_COLS):
        raise ValueError("Отсутствуют базовые колонки в данных.")

    with open(const.PATH_ANSWERS_FILE, 'r', encoding='utf-8') as file:
        answers = json.load(file)

    all_cols = []    

    for section_num in range(2, 8):
        pattern = f'Раздел {section_num}.'
        print(f"Обрабатываю: {pattern}")

        section_cols = [
            col for col in df.columns
            if pattern in col and col not in const.BASE_COLS
        ]
        
        if not section_cols:
            print(f"Предупреждение: Нет колонок для раздела {section_num}.")
            continue

        pattern = pattern.replace('.', '')
        section_answers = answers[pattern]

        for section_col in section_cols:
            all_cols.append(section_col)
            if isinstance(section_answers[section_col], list):
                df[section_col] = df[section_col].astype(str)
                df[section_col] = np.where(df[section_col].isin(section_answers[section_col]), 1, 0)
                df[section_col] = df[section_col].astype(int)
                continue   
            df[section_col] = np.where(df[section_col] == section_answers[section_col], 1, 0)
            df[section_col] = df[section_col].astype(int)

    df['Баллы'] = df[all_cols].sum(axis=1)
    df['Процент правильных ответов'] = (df['Баллы'] / len(all_cols) * 100).round(2).astype(str) + '%'

    cols_to_save = const.BASE_COLS + ['Баллы', 'Процент правильных ответов'] + all_cols
    df = df[cols_to_save]

    return df


def processing_data(df: pd.DataFrame):
    """Обработка данных и разделение на подгруппы"""
    print("Начата вторичная обработка данных.")

    result = {}
    for section_num in range(2, 8):
        pattern = f'Раздел {section_num}.'
        print(f"Обрабатываю: {pattern}")

        section_cols = [
            col for col in df.columns
            if pattern in col and col not in const.BASE_COLS
        ]
        
        if not section_cols:
            print(f"Предупреждение: Нет колонок для раздела {section_num}.")
            continue

        df_temp = df[const.BASE_COLS + section_cols].copy()

        df_temp['Баллы'] = df_temp[section_cols].sum(axis=1)
        df_temp['Процент правильных ответов'] = (df_temp['Баллы'] / len(section_cols) * 100).round(2).astype(str) + '%'

        result[section_num] = df_temp[const.BASE_COLS + ['Баллы', 'Процент правильных ответов'] + section_cols]

    return df, result


def write_results(client: gspread.Client, df_marks: pd.DataFrame, processed_data: dict):
    """Запись результатов на отдельные листы"""
    print("Начата запись результатов на отдельные листы.")
    spreadsheet = client.open_by_key(const.SPREADSHEET_ID)

    sheet_name = "Оценки"
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=sheet_name, 
            rows=max(df_marks.shape[0], 100), 
            cols=max(df_marks.shape[1], 10)
        )

    worksheet.clear()
    set_with_dataframe(worksheet, df_marks)
    print(f"Данные по общей оценке записаны.")
    
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
