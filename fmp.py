import os
import pandas as pd
import requests
import certifi
import json

# Pandas configuration
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 400)


def get_exchange_prices(exchange):
    url = f"https://financialmodelingprep.com/api/v3/symbol/{exchange}?apikey={os.getenv('FMP_API_KEY')}"
    response = requests.get(url, verify=certifi.where())
    data = response.text
    return json.loads(data)


def get_stock_screener():
    url = f"https://financialmodelingprep.com/api/v3/stock-screener?apikey={os.getenv('FMP_API_KEY')}"
    response = requests.get(url, verify=certifi.where())
    data = response.text
    return json.loads(data)


# df_nasdaq = pd.DataFrame(get_exchange_prices('NASDAQ'))
# df_nyse = pd.DataFrame(get_exchange_prices('NYSE'))
# df_amex = pd.DataFrame(get_exchange_prices('AMEX'))
#
# df = pd.concat([df_nasdaq, df_nyse, df_amex], ignore_index=True)
# df.to_csv('data/fmp.csv', index=False)


# diff_sectors = set()
# industries = df_finviz['industry'].unique()
# for industry in industries:
#     df = df_finviz[df_finviz['industry'] == industry]
#     df.reset_index(inplace=True)
#     ticker, industry, sector = df.loc[[0], ['ticker', 'industry', 'sector']].iloc[0]
#     data = get_yahoo_data(ticker)
#     if data is None:
#         print('No data for ', ticker)
#         continue
#     if data['industry'] != industry:
#         print(f"Industry not the same for {ticker} ticker")
#         print('Yahoo industry name: ', data['industry'])
#         print('Finviz industry name: ', industry)
#     if data['sector'] != sector and sector not in diff_sectors:
#         print(f"Sector not the same for {ticker} ticker")
#         print('Yahoo sector name: ', data['sector'])
#         print('Finviz sector name: ', sector)
#         diff_sectors.add(sector)
