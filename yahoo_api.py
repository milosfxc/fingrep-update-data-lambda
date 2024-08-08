import datetime
import yfinance as yf


def get_index_data(ticker: str, date: datetime.date):
    date_str = date.strftime('%Y-%m-%d')
    return yf.download(ticker, start=date_str)