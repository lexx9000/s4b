import json
import pandas as pd
import zipfile
import os
import time
from pathlib import Path

# Список targets содержит имена вендоров, которые будут учитываться при обработке данных.
targets = ["Leanar", "Server-Part", "Система"]

# Получение пути к директории, где находится скрипт
script_dir = os.path.dirname(os.path.abspath(__file__))
csv_file_path = os.path.join(script_dir, 'pricelist.csv')

# CSV-файл pricelist.csv считывается в DataFrame df с определенными параметрами разделителя и кодировки.
df = pd.read_csv(csv_file_path, delimiter=";", encoding="utf-8", keep_default_na=False)


# Переменные startcheck и dataset используются для хранения начального значения и датасета результатов соответственно
startcheck = 0
dataset = []

# Функции для извлечения и обработки данных из JSON-структур
def GetPartnumber(entry):
    return entry[1]

# функция проверки, является ли поставщик подходящим
# подходящие поставщики в массиве targets
def IsValidVendor(entry):
    return GetVendor(entry) in targets

# функция получения наименования поставщика
def GetVendor(entry):
    return entry[len(entry) - 2].split(" ")[0]

# проверка, что значение это число
def IsValidPrice(entry):
    price = GetPrice(entry)
    try:
        int(float(price))
        return True
    except ValueError:
        return False

# функция получения цены в долларах
def GetPrice(entry):
    return entry[len(entry) - 4].replace("~", "")

# Создание датасета
def CreateDataset(productlist):
    for master_entry in productlist:
        # print(master_entry['in'])
        prices = []
        partnumber = ""
        # Этот цикл проходит по каждой строке в разделе listStock текущего master_entry
        for entry in master_entry['listStock']['rows']:
            # Если продукт не проходит валидацию по поставщику цикл переходит к следующей итерации.
            # if not IsValidVendor(entry) or not IsValidPrice(entry):
            #     continue
            if not IsValidPrice(entry):
                continue
            price = int(float(GetPrice(entry)))

            if GetPartnumber(entry).lower() == master_entry['in'].lower():
                prices.append(price)

        for entry in master_entry['listNoStock']['rows']:
            # Если продукт не проходит валидацию по поставщику цикл переходит к следующей итерации.
            # if not IsValidVendor(entry) or not IsValidPrice(entry):
            #     continue
            if not IsValidPrice(entry):
                continue
            price = int(float(GetPrice(entry)))
            if GetPartnumber(entry).lower() == master_entry['in'].lower():
                prices.append(price)

        if prices:
            # price = min(prices)
            # Сортировка массива от меньшего к большему
            sorted_prices = sorted(prices)
            
            
            # Определение итоговой цены
            if len(sorted_prices) >= 3:
                # Взятие 2-й и 3-й цены, их сложение и деление на 2
                price = (sorted_prices[1] + sorted_prices[2]) / 2
                price = int(round(price))
            else:
                # Если цен меньше 3, то наибольшая цена умножается на 0,89
                price = max(sorted_prices) * 0.89
                price = int(round(price))
            
            partnumber = master_entry['in']
            # print(partnumber, sorted_prices)
            pair = (partnumber, price)
            # print(pair)
            dataset.append(pair)

def ExportFiles(szip_name):
    # Извлечение имени файла без пути и расширения
    file_name = os.path.splitext(os.path.basename(szip_name))[0]
    # Путь к целевой директории
    target_dir = os.path.join("./box/", file_name)
    
    # Проверка существования директории
    if os.path.exists(target_dir) and os.path.isdir(target_dir):
        # Очистка содержимого директории
        for filename in os.listdir(target_dir):
            file_path = os.path.join(target_dir, filename)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                elif os.path.isdir(file_path):
                    os.rmdir(file_path)
            except Exception as e:
                print(f"Failed to delete {file_path}: {e}")
    else:
        # Создание директории, если она не существует
        os.mkdir(target_dir)
    
    # Извлечение файлов из ZIP-архива в целевую директорию
    with zipfile.ZipFile(szip_name, 'r') as zip_ref:
        zip_ref.extractall(target_dir)



# Функция удаления пустых строк
def remove_empty_rows(file_path):
    
    # Чтение CSV файла в DataFrame
    df = pd.read_csv(file_path, delimiter=';', encoding='utf-8')

    # Удаление пустых строк
    df.dropna(how='all', inplace=True)

    # Сохранение DataFrame обратно в CSV файл
    df.to_csv(file_path, sep=';', index=False)

# Функция удаления .0
def remove_decimal(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = file.read()

    data = data.replace('.0', '')

    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(data)


# Основной блок кода
def main(extract_files=True):
    zip_dir = os.path.join(script_dir, 'zips')
    path = Path(zip_dir)
    box_dir = os.path.join(script_dir, 'box')
    os.makedirs(box_dir, exist_ok=True)

    # Этап 1: Извлечение файлов из ZIP-архивов (если включено)
    extracted_files = []
    if extract_files:
        for file in path.glob('*.zip'):
            try:
                ExportFiles(file)
                print(f"Extracted: {file}")
                file_name = os.path.splitext(os.path.basename(file))[0]
                extracted_files.append(file_name)  # Сохранение имени файла
            except zipfile.BadZipFile:
                print(f"Error: {file} is not a valid zip file. Skipping to the next file.")
            except Exception as e:
                print(f"An error occurred while processing {file}: {e}")
    else:
        # Если извлечение файлов отключено, используем существующие файлы в директории box
        for dir in os.listdir(box_dir):
            if os.path.isdir(os.path.join(box_dir, dir)):
                extracted_files.append(dir)

    print("All files extracted. Proceeding to dataset creation.")

    # Этап 2: Создание датасета
    global dataset
    dataset = []  # Инициализация списка для хранения данных
    for file_name in extracted_files:
        try:
            with open(os.path.join("./box/", file_name, "res.json"), encoding="utf-8") as jsfile:
                info = json.load(jsfile)['results']
                CreateDataset(info)
                print(f"DataSet Created for {file_name}")
        except Exception as e:
            print(f"An error occurred while creating dataset for {file_name}: {e}")

    # Приведение всех partnumber к нижнему регистру
    dataset_res = [(pair[0].lower(), pair[1]) for pair in dataset]
    print("All Partnumbers to Lower done")
    time.sleep(3)

    # Удаление дубликатов 
    # dataset_res = list(dict.fromkeys(dataset_res))
    # print("Delete duplicates done")
    # time.sleep(3)

    # Приведение partnumber в DataFrame df к нижнему регистру
    df["partnumber"] = df["partnumber"].str.lower()
    print("Partnumber in DataFrame df to lower done")
    time.sleep(3)

    # Обновление цен в DataFrame
    total_rows = len(df)
    print(total_rows)
    for index, row in df.iterrows():
        a = row["partnumber"]
        for pair in dataset_res:
            if pair[0] == a:
                df.loc[index, "S4B цена (USD)"] = pair[1]
        print(f"Processed {index + 1}/{total_rows} rows")

    # Сохранение в CSV
    df.to_csv("newcsv.csv", sep=';', index=False)

    # Удаление пустых строк в CSV файле
    remove_empty_rows('newcsv.csv')

    # Удаление .0 из значений цен
    remove_decimal('newcsv.csv')

    print("New CSV CREATED!")

if __name__ == "__main__":
    main(extract_files=False)  # Установите True или False для включения или отключения извлечения файлов


