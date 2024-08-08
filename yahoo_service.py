import datetime

import pandas as pd

from yahoo_api import get_index_data


def get_indices(tickers: list, date: datetime.date):
    frames = []
    for ticker in tickers:
        df_temp = get_index_data(ticker=ticker, date=date)
        df_temp['ticker'] = ticker
        frames.append(df_temp)
    return pd.concat(frames)
