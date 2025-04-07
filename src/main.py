# Local Libraries
import src.constants as const
from src.utils import *

def main():
    client = auth()
    df = get_source_data(client)
    processed_data = processing_data(df)
    write_results(client, processed_data)

def start_action():
    try:
        main()
        print("Все операции успешно завершены.")
    except Exception as err:
        print(f"Ошибка: {err}.")
