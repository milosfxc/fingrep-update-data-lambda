from datetime import datetime, timedelta
import pandas as pd
import requests
import os
from ta_utils import *

pd.set_option('display.max_rows', 200)


def get_and_insert_aggregated_bars():
    url = ("https://api.polygon.io/v2/aggs/ticker/AACIW/range/1/day/2024-02-01/2024-03-01?adjusted=true&sort=asc"
           "&limit=120")
    params = {
        "apiKey": os.getenv("POLYGON_API_KEY")
    }

    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        df_aggregated_daily = pd.DataFrame(data['results'])
        # Volume column conversion to integer
        df_aggregated_daily['v'] = df_aggregated_daily['v'].astype(int)
        df_aggregated_daily.drop(['n', 'otc'], axis=1, errors='ignore', inplace=True)
        df_aggregated_daily.rename(columns={'v': 'volume', 'o': 'open', 'c': 'close', 'h': 'high', 'l': 'low',
                                            'vw': 'vwap', 't': 'date'},
                                   inplace=True)
        df_aggregated_daily.dropna(subset=['open', 'high', 'low'], inplace=True)
        df_aggregated_daily['volume'] = df_aggregated_daily['volume'].fillna(0)
        df_aggregated_daily['date'] = pd.to_datetime(df_aggregated_daily['date'], unit='ms').dt.date
        print(df_aggregated_daily)


def get_grouped_daily_bars(delay):
    current_utc_date = datetime.utcnow() - timedelta(days=delay)
    current_utc_date = current_utc_date.strftime("%Y-%m-%d")
    url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{current_utc_date}"

    params = {
        "adjusted": "true",
        "include_otc": "false",
        "apiKey": os.getenv("POLYGON_API_KEY")
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        df_grouped_daily_polygon = pd.DataFrame(data['results'])
        # Checks resultsCount and the actual results array length
        # Removes rows that contain at least one NaN OHLC value
        # Removes rows if the ticker column contains anything other than capital letters:
        df_grouped_daily_polygon = df_grouped_daily_polygon[df_grouped_daily_polygon['T'].str.isupper()]
        df_grouped_daily_polygon = df_grouped_daily_polygon[~df_grouped_daily_polygon['T'].str.contains('\.')]
        # Check if all tickers have the same UTC date
        # Volume column conversion to integer
        df_grouped_daily_polygon['date'] = pd.to_datetime(df_grouped_daily_polygon['t'], unit='ms').dt.date
        df_grouped_daily_polygon.drop(columns=['t', 'vw', 'n', 'o', 'c'], inplace=True)
        df_grouped_daily_polygon['v'] = df_grouped_daily_polygon['v'].astype(int)
        df_grouped_daily_polygon.rename(
            columns={'o': 'open', 'c': 'close', 'h': 'high', 'l': 'low'},
            inplace=True)
        return df_grouped_daily_polygon

    else:
        raise Exception(f"Error: {response.status_code} - {response.text}")


def vcp(df: pd.DataFrame):
    df_grouped = df.groupby('T').apply(lambda x: x.reset_index(drop=True))
    conditions_result = df_grouped.groupby(level=0)
    # Group by ticker and check conditions for 'l' and 'h'
    filtered_df = df_grouped.groupby(level=0).filter(
        lambda x: x['l'].iloc[0] == x['l'].min() and x['h'].iloc[0] == x['h'].max())

    # Extract tickers from the filtered dataframe
    desired_tickers = filtered_df.index.get_level_values(0).unique()

    print("Tickers where 'l' has the min value at index 0 and 'h' has the max value at index 0:")
    print(desired_tickers)


def convergence(df: pd.DataFrame):
    return len(df) == 3 and df['low'].iloc[2] == df['low'].min() and df['high'].iloc[2] == df['high'].max()


# df_1 = get_grouped_daily_bars(delay=5)
# df_2 = get_grouped_daily_bars(delay=4)
# df_3 = get_grouped_daily_bars(delay=3)
# df = pd.concat([df_1, df_2, df_3], axis=0)

# Sort the DataFrame by date in descending order within each group
# df.sort_values(by='date', ascending=False, inplace=True)
# Filter the groups based on the consolidation function
# convergence_tickers = df.groupby('T').apply(convergence, include_groups=False)
# df = pd.merge(df, convergence_tickers.rename('convergence'), on=['T'], how='left')

# # Add a new column 'vcp' indicating whether the rows fulfill the consolidation condition
# filtered_df['vcp'] = True
# print(filtered_df.head())
# # Reset index
# filtered_df.reset_index(drop=True, inplace=True)
# print(filtered_df.head())
# # Display the resulting DataFrame
# print(filtered_df)
# get_and_insert_aggregated_bars()
