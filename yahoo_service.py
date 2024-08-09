import datetime

import pandas as pd

import yahoo_api
from yahoo_api import request_ohlc_data


def get_indices(tickers: list, date: datetime.date):
    frames = []
    for ticker in tickers:
        df_temp = request_ohlc_data(ticker=ticker, date=date)
        df_temp.drop(columns=['Adj Close'], inplace=True)
        df_temp.reset_index(inplace=True)
        df_temp.rename(columns={"Date": "date", "Open": "open", "High": "high", "Low": "low", "Close": "close",
                                "Volume": "volume"}, inplace=True)
        df_temp['ticker'] = ticker
        frames.append(df_temp)
    return pd.concat(frames)


def get_index_details(tickers: list):
    data = []
    for ticker in tickers:
        ticker_details = yahoo_api.request_ticker_details(ticker)
        if ticker_details:
            data.append({
                "ticker": ticker,
                "name": ticker_details.get('shortName'),
                "currency": ticker_details.get('currency')
            })
    return pd.DataFrame(data)