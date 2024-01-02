import pandas as pd
import requests

df = pd.read_csv('data/stocks.csv')
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 400)

# url = "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/2023-12-14"
# params = {
#     "adjusted": "true",
#     "include_otc": "false",
#     "apiKey": "QQt28XYmrVXC4b_Cv2cWRWXlNJ5wVMy3"
# }
#
# response = requests.get(url, params=params)
#
# if response.status_code == 200:
#     data = response.json()
#     df = pd.DataFrame(data['results'])
#     df.to_csv('data/polygon.csv', index=False)
#     # Process the data as needed
# else:
#     print(f"Error: {response.status_code} - {response.text}")
df_polygon = pd.read_csv('data/polygon.csv')
df_nasdaq = pd.read_csv('data/stocks.csv')
df_fmp = pd.read_csv('data/fmp.csv')
df_fmp['timestamp'] = pd.to_datetime(df_fmp['timestamp'], unit='s')
target_time = pd.to_datetime('2023-11-14 21:00:00')
df_fmp = df_fmp[df_fmp['timestamp'] >= target_time]
#print(df_fmp.query('timestamp >= @target_time'))
#ZLS and AACI
common_stocks_polygon = df_polygon[df_polygon['T'].str.isupper()]
print(len(common_stocks_polygon))
print(len(df_polygon))
common_tickers_with_fmp = df_nasdaq[df_nasdaq['Symbol'].isin(df_fmp['symbol'])]
common_tickers_with_polygone = df_nasdaq[df_nasdaq['Symbol'].isin(common_stocks_polygon['T'])]
polygon_tickers_in_fmp = df_polygon[df_polygon['T'].isin(df_fmp['symbol'])]
print(len(common_tickers_with_polygone))
print(len((common_tickers_with_fmp)))
print(len(polygon_tickers_in_fmp))

# Assuming df is your DataFrame and 'tickers' is the column with ticker symbols
df_nasdaq_non_alphabetic = df_nasdaq[df_nasdaq['Symbol'].str.contains('[^a-zA-Z]', na=False)]
#print(df_nasdaq_non_alphabetic)
df_polygon_non_alphabetic = df_polygon[df_polygon['T'].str.contains('[a-z]', na=False)]
df_fmp_non_alphabetic = df_fmp[df_fmp['symbol'].str.contains('PORT', na=False)]
