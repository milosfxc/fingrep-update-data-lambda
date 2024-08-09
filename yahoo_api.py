import datetime
import logging
import requests
import yfinance as yf
from requests.exceptions import HTTPError
import inspect


def request_ohlc_data(ticker: str, date: datetime.date):
    date_str = date.strftime('%Y-%m-%d')
    return yf.download(ticker, start=date_str)


def request_ticker_details(ticker):
    ticker_info = None
    try:
        ticker_info = yf.Ticker(ticker).info
        if len(ticker_info) < 10:
            raise Exception
        return ticker_info
    except Exception as e:
        current_method = inspect.currentframe().f_code.co_name
        class_name = __name__
        logging.error(f"Error in {class_name}.{current_method}: Method returned ticker_info: {ticker_info}. Exception: {e}")
    return None




