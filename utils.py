import os
import pandas as pd
import yfinance as yf
import requests
from selenium.webdriver import Keys
from selenium.webdriver.common.by import By
import undetected_chromedriver as uc
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import numpy as np
# Pandas configuration
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 400)


def generate_insert_statement(table_name, column_names, values):
    sql_statement = f"INSERT INTO {table_name} ({column_names[0]}) VALUES "
    for i in range(0, len(values)):
        value = f"('{values[i][0]}', '{values[i][1]}', '{values[i][2]}'), "
        sql_statement = sql_statement + value
    return sql_statement


def get_yahoo_sector_and_industry(ticker):
    ans = dict()
    try:
        data = yf.Ticker(ticker).info
        if 'industry' not in data or 'sector' not in data:
            print(f"utils#get_yahoo_sector_and_industry#{ticker}: couldn't find industry and sector keys")
            return None
        ans['industry'] = data['industry']
        ans['sector'] = data['sector']
    except requests.exceptions.HTTPError as httpError:
        print(f"HTTPError for ticker {ticker}: {httpError}")
        ans = None
    return ans


def get_finviz_sector_and_industry_and_country(ticker):
    ans = dict()
    options = uc.ChromeOptions()
    options.page_load_strategy = 'none'
#    options.add_argument("--headless")
#    options.add_argument('whitelisted-ips')
#    options.add_argument("no-sandbox")
    options.add_argument("disable-dev-shm-usage")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-page-load-strategy")
#    options.add_argument("enable-automation")
    options.add_argument("--disable-browser-side-navigation")
#    options.add_argument("--disable-web-security")
#    options.add_argument("--disable-dev-shm-usage")
    options.add_argument('--disable-popup-blocking')
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-gpu")
    driver = uc.Chrome(version_main=120, options=options)
    driver.get(f'https://finviz.com/quote.ashx?t={ticker}&ty=c&ta=1&p=d')
    driver.implicitly_wait(1)
    wait = WebDriverWait(driver, 5)

    wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body'))).send_keys(Keys.NULL)  # This line is to make sure the body is focused
    wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body'))).send_keys(Keys.NULL)  # This line is to make sure the body is focused
    wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body'))).send_keys(Keys.NULL)  # This line is to make sure the body is focused
    wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body'))).send_keys(    Keys.NULL)  # This line is to make sure the body is focused
    element = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'quote-links')))
    if not element:
        print(f"utils#get_finviz_sector_and_industry#{ticker}: couldn't find the element by its class name")
        return None
    elements = element.text.split('•')
    if len(elements) < 2:
        print(f"utils#get_finviz_sector_and_industry#{ticker}: elements array was empty")
        return None

    ans['sector'] = elements[0].strip()
    ans['industry'] = elements[1].strip()
    ans['country'] = elements[2].strip()
    driver.quit()
    return ans


def get_polygon_undefined():
    ticker_types_url = "https://api.polygon.io/v3/reference/exchanges?asset_class=stocks"
    params = {
        "apiKey": os.getenv("POLYGON_API_KEY")
    }
    response = requests.get(ticker_types_url, params=params)
    if response.status_code == 200:
        ticker_types_data = response.json()
        return pd.DataFrame(ticker_types_data['results'])
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None

def apr_joke():
    url = "https://pretraga2.apr.gov.rs/unifiedentitysearch"
    options = uc.ChromeOptions()
    options.page_load_strategy = 'none'
    #    options.add_argument("--headless")
    #    options.add_argument('whitelisted-ips')
    #    options.add_argument("no-sandbox")
    options.add_argument("disable-dev-shm-usage")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-page-load-strategy")
    #    options.add_argument("enable-automation")
    options.add_argument("--disable-browser-side-navigation")
    #    options.add_argument("--disable-web-security")
    #    options.add_argument("--disable-dev-shm-usage")
    options.add_argument('--disable-popup-blocking')
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-gpu")
    driver = uc.Chrome(version_main=120, options=options)
    driver.get(url)
    driver.implicitly_wait(1)
    wait = WebDriverWait(driver, 5)

    wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body'))).send_keys(
        Keys.NULL)  # This line is to make sure the body is focused
    wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body'))).send_keys(
        Keys.NULL)  # This line is to make sure the body is focused
    wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body'))).send_keys(
        Keys.NULL)  # This line is to make sure the body is focused
    wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body'))).send_keys(
        Keys.NULL)  # This line is to make sure the body is focused
    element = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'quote-links')))
#
# table = 'exchanges'
# columns = ['exchange_name', 'mic', 'operating_mic']
# values = df[['name', 'mic', 'operating_mic']].values.tolist()
#
# statm = generate_insert_statement(table, columns, values)
# print(statm)


def atr_new_ticker(data, period=14):
    # Calculate True Range (TR)
    data['High-Low'] = data['high'] - data['low']
    data['High-PrevClose'] = np.abs(data['high'] - data['close'].shift(1))
    data['Low-PrevClose'] = np.abs(data['low'] - data['close'].shift(1))

    data['TrueRange'] = np.maximum.reduce([data['High-Low'], data['High-PrevClose'], data['Low-PrevClose']])

    # Calculate Average True Range (ATR)
    data['abs_atr'] = data['TrueRange'].rolling(window=period).mean()

    # Drop intermediate columns
    data = data.drop(['High-Low', 'High-PrevClose', 'Low-PrevClose', 'TrueRange'], axis=1)
    return data['abs_atr']


def rsi_tradingview_new_ticker(ohlc: pd.DataFrame, period: int = 14, round_rsi: bool = True):

    delta = ohlc["close"].diff()

    up = delta.copy()
    up[up < 0] = 0
    up = pd.Series.ewm(up, alpha=1/period).mean()
    down = delta.copy()
    down[down > 0] = 0
    down *= -1
    down = pd.Series.ewm(down, alpha=1/period).mean()
    rsi = np.where(up == 0, 0, np.where(down == 0, 100, 100 - (100 / (1 + up / down))))
    arr = np.round(rsi, 2) if round_rsi else rsi
    arr[:period] = np.nan
    return arr


def rsi_tradingview_existing_ticker(ohlc: pd.DataFrame, period: int = 14, round_rsi: bool = True):

    delta = ohlc["close"].diff()
    up = delta.copy()
    up[up < 0] = 0
    up = pd.Series.ewm(up, alpha=1/period).mean()
    down = delta.copy()
    down[down > 0] = 0
    down *= -1
    down = pd.Series.ewm(down, alpha=1/period).mean()
    rsi = np.where(up == 0, 0, np.where(down == 0, 100, 100 - (100 / (1 + up / down))))
    return np.round(rsi, 2) if round_rsi else rsi


