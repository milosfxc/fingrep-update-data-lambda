import datetime
import os
from db_ops import postgres_engine, get_all
import pandas as pd
from datetime import datetime, timedelta
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 400)
from ta_utils import rsi_tv_new_tickers, atr_new_tickers


def get_and_insert_aggregated_bars():
    data = get_all()
    for share_id, group in data.groupby('share_id'):
        group.sort_values(by='date', ascending=False)
        group['volume'] = group['volume'].fillna(0)
        group['rsi'] = rsi_tv_new_tickers(group)
        group = atr_new_tickers(group)
        group['rel_atr'] = (group['abs_atr'] / group['close'] * 100).round(2)
        print(group)
        try:
            group.to_sql('d_timeframe', con=postgres_engine(), if_exists='append', index=False, chunksize=1000,
                         method='multi', index_label=['share_id', 'date'])
        except Exception as e:
            print(f"#get_and_insert_aggregated_bars#{share_id}#Database insertion error: {e}")
            raise


get_and_insert_aggregated_bars()

# if response.status_code == 200:
#     data = response.json()
#     df_aggregated_daily = pd.DataFrame(data['results'])
#     # Volume column conversion to integer
#     df_aggregated_daily['v'] = df_aggregated_daily['v'].astype(int)
#     df_aggregated_daily['share_id'] = ticker_id
#     df_aggregated_daily.drop(['n', 'otc'], axis=1, errors='ignore', inplace=True)
#     df_aggregated_daily.rename(columns={'v': 'volume', 'o': 'open', 'c': 'close', 'h': 'high', 'l': 'low',
#                                         'vw': 'vwap', 't': 'date'},
#                                inplace=True)
#     df_aggregated_daily.dropna(subset=['open', 'high', 'low'], inplace=True)
#     df_aggregated_daily['volume'] = df_aggregated_daily['volume'].fillna(0)
#     df_aggregated_daily['date'] = pd.to_datetime(df_aggregated_daily['date'], unit='ms').dt.date
#     df_aggregated_daily['rsi'] = rsi_tv_new_tickers(df_aggregated_daily.copy())
#     df_aggregated_daily['abs_atr'] = atr_new_tickers(df_aggregated_daily.copy())
#     df_aggregated_daily['rel_atr'] = (df_aggregated_daily['abs_atr'] / df_aggregated_daily['close'] * 100).round(2)
#     # df_aggregated_daily['avg_volume'] = df_aggregated_daily['volume'].rolling(window=20).mean()
#     # df_aggregated_daily['sma10'] = df_aggregated_daily['close'].rolling(window=10).mean().round(4)
#     # df_aggregated_daily['sma20'] = df_aggregated_daily['close'].rolling(window=20).mean().round(4)
#     # df_aggregated_daily['sma50'] = df_aggregated_daily['close'].rolling(window=50).mean().round(4)
#     # df_aggregated_daily['sma100'] = df_aggregated_daily['close'].rolling(window=100).mean().round(4)
#     # df_aggregated_daily['sma200'] = df_aggregated_daily['close'].rolling(window=200).mean().round(4)
#     # df_aggregated_daily['adr(%)'] = (100 * ((df_aggregated_daily['high'] / df_aggregated_daily['low'])
#     #                                       .rolling(window=20).mean() - 1)).round(2)
#     # df_aggregated_daily['adr($)'] = ((df_aggregated_daily['high'] - df_aggregated_daily['low'])
#     #                                 .rolling(window=20).mean()).round(2)
#     # df_aggregated_daily['rel_volume'] = (df_aggregated_daily['volume']/df_aggregated_daily['avg_volume']).round(2)
#     # df_aggregated_daily['rsi'] = utils.rsi_tradingview(df_aggregated_daily)
#     # df_aggregated_daily['atr'] = utils.calculate_atr(df_aggregated_daily)
#     # print(df_aggregated_daily['atr'])
#     try:
#         df_aggregated_daily.to_sql('d_timeframe', con=postgres_engine(), if_exists='append', index=False,
#                                    index_label=['share_id', 'date'])
#     except Exception as e:
#         print(f"#get_and_insert_aggregated_bars#{ticker}#Database insertion error: {e}")
#         raise
# else:
#     print(f"#get_and_insert_aggregated_bars: {response.status_code} - {response.text}")
