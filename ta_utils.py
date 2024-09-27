import pandas as pd
import numpy as np

# Pandas configuration
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 400)


def generate_insert_statement(table_name, column_names, values):
    sql_statement = f"INSERT INTO {table_name} ({column_names[0]}) VALUES "
    for i in range(0, len(values)):
        value = f"('{values[i]}'), "
        sql_statement = sql_statement + value
    return sql_statement


def atr_new_tickers(data, period=14):
    # Calculate True Range (TR)
    data['High-Low'] = data['high'] - data['low']
    data['High-PrevClose'] = np.abs(data['high'] - data['close'].shift(1))
    data['Low-PrevClose'] = np.abs(data['low'] - data['close'].shift(1))

    data['TrueRange'] = np.maximum.reduce([data['High-Low'], data['High-PrevClose'], data['Low-PrevClose']])

    # Calculate Average True Range (ATR)
    data['abs_atr'] = data['TrueRange'].rolling(window=period).mean().round(4)

    # Drop intermediate columns
    data = data.drop(['High-Low', 'High-PrevClose', 'Low-PrevClose', 'TrueRange'], axis=1)
    return data['abs_atr']


def atr_existing_tickers(data, period=14):
    if len(data) > period:
        data = data.tail(15).copy()
        # Calculate True Range (TR)
        data['High-Low'] = data['high'] - data['low']
        data['High-PrevClose'] = np.abs(data['high'] - data['close'].shift(1))
        data['Low-PrevClose'] = np.abs(data['low'] - data['close'].shift(1))

        data['TrueRange'] = np.maximum.reduce([data['High-Low'], data['High-PrevClose'], data['Low-PrevClose']])

        # Calculate Average True Range (ATR)
        data['abs_atr'] = data['TrueRange'].rolling(window=period).mean().round(4)

        # Drop intermediate columns
        data = data.drop(['High-Low', 'High-PrevClose', 'Low-PrevClose', 'TrueRange'], axis=1)
        return data[['abs_atr']]
    else:
        return pd.DataFrame(columns=['abs_atr'])


def rsi_tv_new_tickers(ohlc: pd.DataFrame, period: int = 14):

    delta = ohlc["close"].diff()

    up = delta.copy()
    up[up < 0] = 0
    up = pd.Series.ewm(up, alpha=1/period).mean()
    down = delta.copy()
    down[down > 0] = 0
    down *= -1
    down = pd.Series.ewm(down, alpha=1/period).mean()
    rsi = np.where(up == 0, 0, np.where(down == 0, 100, 100 - (100 / (1 + up / down))))
    arr = np.round(rsi, 2)
    arr[:period] = np.nan
    return arr


def rsi_tv_existing_tickers(ohlc: pd.DataFrame, period: int = 14):

    delta = ohlc["close"].diff()
    up = delta.copy()
    up[up < 0] = 0
    up = pd.Series.ewm(up, alpha=1/period).mean()
    down = delta.copy()
    down[down > 0] = 0
    down *= -1
    down = pd.Series.ewm(down, alpha=1/period).mean()
    rsi = np.where(up == 0, 0, np.where(down == 0, 100, 100 - (100 / (1 + up / down))))
    arr = np.round(rsi, 2)
    arr[:period] = np.nan
    rsi_df = pd.DataFrame(arr, columns=["rsi"], index=ohlc.index)
    return rsi_df


def convergence(df: pd.DataFrame):
    df_last_3 = df.copy()
    df_last_3.sort_values(by='date', ascending=False, inplace=True)
    df_last_3 = df_last_3.head(3)
    return (len(df_last_3) == 3 and df_last_3['low'].iloc[0] == df_last_3['low'].min() and
            df_last_3['high'].iloc[0] == df_last_3['high'].max())


def atr_existing_tickers_v2(data, period=14):
    for share_id, group in data.groupby('share_id'):
        if len(group) > 14:
            group = group.tail(15).copy()
            # Calculate True Range (TR)
            group['High-Low'] = group['high'] - group['low']
            group['High-PrevClose'] = np.abs(group['high'] - group['close'].shift(1))
            group['Low-PrevClose'] = np.abs(group['low'] - group['close'].shift(1))

            group['TrueRange'] = np.maximum.reduce([group['High-Low'], group['High-PrevClose'], group['Low-PrevClose']])

            # Calculate Average True Range (ATR)
            group['abs_atr'] = group['TrueRange'].rolling(window=period).mean().round(4)

            # Drop intermediate columns
            group = group.drop(['High-Low', 'High-PrevClose', 'Low-PrevClose', 'TrueRange'], axis=1)
            data.loc[group.index, 'abs_atr'] = group['abs_atr']
    return data['abs_atr']

