# Local Libraries
import src.constants as const
from src.utils import *

def main():
    client = auth()
    df = get_source_data(client)
    preprocessed_data = preprocessing_data(df)
    preprocessed_data, processed_data = processing_data(preprocessed_data)
    write_results(client=client, df_marks=preprocessed_data, processed_data=processed_data)

def start_action():
    try:
        main()
        print("Все операции успешно завершены.")
    except Exception as err:
        print(f"Ошибка: {err}.")
