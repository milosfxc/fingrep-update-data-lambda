import datetime

import pandas as pd
import yahoo_api
from yahoo_api import request_ohlc_data


def get_index_details(tickers: list, currency_mapping: dict):
    data = []
    for ticker in tickers:
        ticker_details = yahoo_api.request_ticker_details(ticker)
        if ticker_details:
            data.append({
                "ticker": ticker[1:],
                "name": ticker_details.get('shortName'),
                "currency_id": currency_mapping.get(ticker_details.get('currency'))
            })
    return pd.DataFrame(data)


def get_indices_ohlcv(tickers: list, date: datetime.date, indices_mapping: dict):
    frames = []
    for ticker in tickers:
        df_temp = request_ohlc_data(ticker=ticker, date=date)
        df_temp.columns = df_temp.columns.droplevel(level='Ticker')
        df_temp.reset_index(inplace=True)
        df_temp.rename(columns={"Date": "date", "Open": "open", "High": "high", "Low": "low", "Close": "close",
                                "Volume": "volume"}, inplace=True)
        df_temp['index_id'] = indices_mapping.get(ticker[1:])
        frames.append(df_temp)
    return pd.concat(frames)
