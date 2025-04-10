# Standard Libraries
import os
from pathlib import Path

# Third Party Libraries
from dotenv import load_dotenv

load_dotenv()

DEBUG = False

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

if DEBUG:
    SPREADSHEET_ID = os.getenv("SPREADSHEET_ID_TEST")
else:
    SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SOURCE_SHEET_NAME = os.getenv("SOURCE_SHEET_NAME")
BASE_COLS = ['ФИО', 'Группа']

PROJECT_FOLDER = Path.cwd()
CREDS_FOLDER = PROJECT_FOLDER / "creds"
PATH_CREDS_FILE = CREDS_FOLDER / "credentials.json"
PATH_TOKEN_FILE = CREDS_FOLDER / "token.json"
WORK_FILE_FOLDER = PROJECT_FOLDER / "work-file-dir"
PATH_ANSWERS_FILE = WORK_FILE_FOLDER / "answers.json"
