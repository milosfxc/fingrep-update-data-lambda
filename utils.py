import time

import yfinance as yf
import requests
from selenium.webdriver.common.by import By
import undetected_chromedriver as uc

def generate_insert_statement(table_name, column_names, values):
    sql_statement = f"INSERT INTO {table_name} ({column_names[0]}) VALUES "
    for i in range(0, len(values)):
        value = f"('{values[i]}'), "
        sql_statement = sql_statement + value
    return sql_statement


def get_yahoo_sector_and_industry(ticker):
    ans = dict()
    try:
        data = yf.Ticker(ticker).info
        if 'industry' not in data or 'sector' not in data:
            return None
        ans['industry'] = data['industry']
        ans['sector'] = data['sector']
    except requests.exceptions.HTTPError as httpError:
        print(f"HTTPError for ticker {ticker}: {httpError}")
        ans = None
    return ans


def get_finviz_sector_and_industry(ticker):
    ans = dict()
    options = uc.ChromeOptions()
#    options.add_argument("--headless")
#    options.add_argument('whitelisted-ips')
#    options.add_argument("no-sandbox")
    options.add_argument("disable-dev-shm-usage")
    options.add_argument("--disable-extensions")
#    options.add_argument("enable-automation")
    options.add_argument("--disable-browser-side-navigation")
#    options.add_argument("--disable-web-security")
#    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-gpu")
    driver = uc.Chrome(version_main=120, options=options)
    driver.get(f'https://finviz.com/quote.ashx?t={ticker}&ty=c&ta=1&p=d')
    element = driver.find_elements(By.CLASS_NAME, 'quote-links')
    if not element:
        print("utils#get_finviz_sector_and_industry: couldn't find the element by its class name")
        return None
    elements = element[0].text.split('•')
    if len(elements) < 2:
        print("utils#get_finviz_sector_and_industry: elements array was empty")
        return None
    ans['sector'] = elements[0].strip()
    ans['industry'] = elements[1].strip()
    driver.quit()
    return ans
