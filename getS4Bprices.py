import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import fake_useragent

# Корневая директория
script_dir = os.path.dirname(os.path.abspath(__file__))

# Настройки пути для скачивания
download_dir = os.path.join(script_dir, 'sup-prices')

# Список прайсов для скачивания
price_list = [
    {"name": "01-Noc-Technology.zip", "url": "https://s4b.ru/s.jsp?a=5223&disId=1324", "prefix": "1521"},
    {"name": "02-Alfa-Networks.zip", "url": "https://s4b.ru/s.jsp?a=5223&disId=920", "prefix": "1072"},
    {"name": "03-Pixma.zip", "url": "https://s4b.ru/s.jsp?a=5223&disId=1295", "prefix": "1491"},
    {"name": "04-SI-Partners.zip", "url": "https://s4b.ru/s.jsp?a=5223&disId=926", "prefix": "1081"},
    {"name": "05-Gravikom.zip", "url": "https://s4b.ru/s.jsp?a=5223&disId=1327", "prefix": "1524"},
    {"name": "06-Serverteh.zip", "url": "https://s4b.ru/s.jsp?a=5223&disId=1018", "prefix": "1187"},
    {"name": "07-Telea.zip", "url": "https://s4b.ru/s.jsp?a=5223&disId=928", "prefix": "1083"}
]

# Настройки Selenium
options = webdriver.ChromeOptions()
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
user = fake_useragent.UserAgent().random
options.add_argument(f'user-agent={user}')  # Использование случайного User-Agent

# Настройки для автоматической загрузки файлов
prefs = {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
}
options.add_experimental_option("prefs", prefs)

# Инициализация браузера
driver = webdriver.Chrome(options=options)

# URL для авторизации
login_url = "https://s4b.ru/s.jsp?a=10025"
driver.get(login_url)

# Авторизация
username = driver.find_element(By.NAME, "usrLogin")
password = driver.find_element(By.NAME, "usrPassword")
remember_me = driver.find_element(By.NAME, "isRemember")

username.send_keys("Worksys")
password.send_keys("66119-1260")
remember_me.click()
password.send_keys(Keys.RETURN)

# Ожидание завершения авторизации и загрузки новой страницы
WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

# Функция для скачивания и переименования файла
def download_and_rename(price_info):
    # Переход на страницу прайса
    driver.get(price_info["url"])

    # Ожидание загрузки страницы
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

    # Получение HTML-контента страницы
    info_response = driver.page_source

    # Парсинг HTML-контента с использованием BeautifulSoup
    soup = BeautifulSoup(info_response, 'lxml')

    # Ищем тег <a> с текстом "прайс" внутри
    price_link = soup.find('a', string='прайс')

    # Получаем значение атрибута href и формируем полный URL
    if price_link:
        download_url = 'https://s4b.ru/' + price_link['href']
        print("Ссылка на скачивание прайса:", download_url)

        # Переход по ссылке для скачивания прайса
        driver.get(download_url)

        # Ожидание завершения скачивания
        while True:
            time.sleep(1)  # Ждем 1 секунду перед повторной проверкой
            if not any([filename.endswith('.crdownload') for filename in os.listdir(download_dir)]):
                break

        # Поиск и переименование загруженного файла
        files = os.listdir(download_dir)
        for file_name in files:
            if file_name.startswith(price_info["prefix"]):  # Проверка префикса файла
                old_file_path = os.path.join(download_dir, file_name)
                new_file_path = os.path.join(download_dir, price_info["name"])
                os.rename(old_file_path, new_file_path)
                print(f"Файл переименован в: {price_info['name']}")
                break
    else:
        print(f"Ссылка на скачивание прайса для {price_info['name']} не найдена")

# Скачивание и переименование всех прайсов
for price_info in price_list:
    download_and_rename(price_info)


# Закрытие браузера
driver.quit()

