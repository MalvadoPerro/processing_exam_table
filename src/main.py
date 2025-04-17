# Standard Libraries
import time

# Local Libraries
import src.constants as const
from src.utils import *

def main():
    time_limit = int(input('Введите кол-во минут, по истечении которых скрипт должен отключиться: '))
    print(f"Скрипт запущен и завершится через {time_limit} минут(ы).")
    time_limit = time_limit * 60
    start_time = time.time()
    
    client = auth()

    answers_count = 0

    while True:
        if time.time() - start_time >= time_limit:
            break

        df = get_source_data(client)

        if len(df) > answers_count:
            answers_count = len(df)
            
            preprocessed_data = preprocessing_data(df)
            preprocessed_data, processed_data = processing_data(preprocessed_data)
            write_results(client=client, df_marks=preprocessed_data, processed_data=processed_data)

        time.sleep(5)

    print("Скрипт завершил работу по истечении заданного времени.")


def start_action():
    try:
        main()
        print("Все операции успешно завершены.")
    except Exception as err:
        print(f"Ошибка: {err}.")
