from massive import WebSocketClient
from massive.websocket.models import WebSocketMessage, Feed, Market
from typing import List
import pandas as pd

import config
import db_ops
import edgar_service_v2
import fingrep_service
import fundamentals_service
import utils

client = WebSocketClient(
	api_key="QQt28XYmrVXC4b_Cv2cWRWXlNJ5wVMy3",
	feed=Feed.Delayed,
	market=Market.Stocks
	)

# aggregates (per minute)
client.subscribe("AM.*") # single ticker
# client.subscribe("AM.*") # all tickers
# client.subscribe("AM.AAPL") # single ticker
# client.subscribe("AM.AAPL", "AM.MSFT") # multiple tickers

if __name__ == "__main__":
    # calc = pd.to_datetime(1777248000000, unit='ms')
    # print(calc)
    # print(utils.get_utc_date(days=config.days))
    # df = edgar_service_v2.get_latest_filings(f"{utils.get_utc_date(days=config.days + 1)}:{utils.get_utc_date(days=config.days)}")
    # print(df.head())
    # print(df.tail())
    # ticker_and_share_id_by_cik = db_ops.get_foreign_keys()['ticker_and_share_id_by_cik']
    # print(ticker_and_share_id_by_cik)
    # processed_filings = db_ops.get_accession_numbers()
    # print(processed_filings)

    # start_date, end_date = utils.get_utc_date(days=config.days + 1), utils.get_utc_date(days=config.days)
    # df_filings = edgar_service_v2.get_latest_filings(f"{start_date}:{end_date}")
    # print(df_filings.head())
    # print(df_filings.tail())
    # for index, row in df_filings.iterrows():
    #     print(index)
    # processed_filings = db_ops.get_accession_numbers(start_date, end_date)
    # print(isinstance(processed_filings, set))
    db_ops.alter_d_timeframe_triggers(full='DISABLE', compact='ENABLE')
    # db_ops.alter_d_timeframe_triggers(full='ENABLE', compact='DISABLE')
