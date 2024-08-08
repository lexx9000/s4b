import requests
import json
import csv
import time
import pandas as pd
from itertools import islice
import os
import sys
from datetime import datetime
from urllib.parse import quote
import zipfile

username = "Worksys"
password = "66119-1260"
request_body = "http://s4b.ru/s.jsp?a=10041&at=3&usrLogin=" + username + "&usrPassword=" + password + "&sr="

# Определение пути к текущему скрипту и к файлу pricelist.csv
script_dir = os.path.dirname(os.path.abspath(__file__))
csv_file_path = os.path.join(script_dir, 'pricelist.csv')

# Чтение CSV-файла в DataFrame с использованием pandas.
df = pd.read_csv(csv_file_path, delimiter=";", quoting=csv.QUOTE_ALL, encoding="utf-8", keep_default_na=False)

# Вывод первых строк DataFrame для проверки содержимого
# print(df.head())

# Инициализация переменных для отслеживания прогресса и размера чанка.
startcheck = megacount = 0
chunk_size = 98

# функция проверки целостности zip-архива
def is_zipfile_valid(filepath):
    try:
        with zipfile.ZipFile(filepath, 'r') as zip_ref:
            bad_file = zip_ref.testzip()
            if bad_file is not None:
                print(f"Corrupt file found in the archive: {bad_file}")
                return False
        return True
    except zipfile.BadZipFile:
        print(f"Bad zip file: {filepath}")
        return False
    
# запись партномеров в текстовый файл, если не удаётся скачать архив или файл битый
def log_failed_request(finrequest, parts, reason, log_file='failed_requests.txt'):
    with open(log_file, 'a') as f:
        f.write(f"Request: {finrequest}\n")
        f.write(f"Parts: {','.join(map(str, parts))}\n")
        f.write(f"Reason: {reason}\n")
        f.write("\n")

# Эта функция формирует URL-запрос и загружает файл по указанным номерам деталей.
# Объявление функции GetFile, которая принимает два параметра: file: имя файла для сохранения, parts: список номеров деталей.
def GetFile(file, parts,  max_retries=3):
    # Инициализация переменной finrequest значением глобальной переменной request_body, содержащей базовую часть URL для запроса.
    finrequest = request_body
    # Цикл for проходит по каждому номеру в списке parts
    for partnumber in parts:
        # кодирует номер детали для включения в URL с использованием функции quote из urllib.parse. 
        # Это необходимо для корректного формирования URL, особенно если номера деталей содержат специальные символы.
        encoded_partnumber = quote(str(partnumber))
        # добавляет номер и символ ; в конец строки запроса.
        finrequest += encoded_partnumber + ";"

    for attempt in range(max_retries):
        # Отправка HTTP-запроса методом GET к серверу с сформированным URL finrequest.
        # Параметр stream=True указывает, что содержимое ответа должно передаваться в потоковом режиме, что полезно для загрузки больших файлов.
        response = requests.request("GET", finrequest, stream=True)

        if response.status_code == 200:
            filepath = f"./zips/{file}.zip"
            # открытие файла для записи в бинарном режиме ('wb'). 
            # Имя файла формируется из параметра file и расширения .zip. Файл будет сохранен в папке ./zips/.
            with open(filepath, 'wb') as file:
                # запись содержимого ответа (response.content) в открытый файл.
                file.write(response.content)
            
            # проверка что zip файл хороший
            if is_zipfile_valid(filepath):
                print("Zip file is good.")
                return True
            
            # если нет, удалить его и сделать запись в текстовый файл
            else:
                print(f"Invalid zip file: {filepath}")
                os.remove(filepath)  # Удаление битого файла
                log_failed_request(finrequest, parts, "Corrupt file")
            
        else:
            print(f"Attempt {attempt + 1} failed to download {file}. Status code: {response.status_code}")
            time.sleep(5)  # Задержка перед повторной попыткой
    
    # Запись информации о неудачном запросе в лог-файл
    log_failed_request(finrequest, parts, "Failed after max retries")
    print(f"Failed to download {file} after {max_retries} attempts. Logged the request.")
    return False

# Основная функция обработки
def processEverything():
    global df, megacount, startcheck, chunk_size
    
    print("Script starts working...")
    
    # пустой список для хранения парт-номеров
    pn_array = []
    # счетчик для отслеживания количества добавленных номеров
    counter = 0
    # общее количество строк в DataFrame
    total_rows = len(df)
    # счетчик для отслеживания количества обработанных строк
    processed_rows = 0
    
    # Использование islice для итерации по строкам DataFrame, начиная с megacount * chunk_size. df.iterrows() возвращает индекс и строку, по которым идет итерация.
    for index, row in islice(df.iterrows(), megacount * chunk_size, None):
        # a - получение значения из столбца "partnumber" текущей строки.
        a = row["partnumber"]
        # Если значение пустое, выполнение перехода к следующей итерации с помощью continue.
        if a == "":
            continue

        # Если счетчик меньше chunk_size, парт-номер добавляется в pn_array
        if counter < chunk_size:
            pn_array.append(a)
            counter += 1
        else:
            # Если счетчик достигает chunk_size, увеличивается megacount.
            megacount += 1
            # Если megacount больше startcheck, вызывается функция GetFile с текущим номером мегачанка и последними 100 номерами деталей.
            if megacount > startcheck:
                success = GetFile(str(megacount), pn_array[-100:])
                # Добавляется задержка в 11 секунд для предотвращения перегрузки сервера.

                if not success:
                    print(f"Failed to download file for megacount {megacount}. Skipping...")

                time.sleep(11)
                # Очищаются pn_array и сбрасывается counter.
                pn_array = []
                counter = 0
        
        processed_rows += 1
        if processed_rows % 100 == 0:
            current_time = datetime.now().strftime("%H:%M:%S")
            print(f"[{current_time}] Left parts: {total_rows - processed_rows}")
    
    print("All tasks DONE!")


processEverything()
