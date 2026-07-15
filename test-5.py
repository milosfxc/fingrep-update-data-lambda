import datetime

from massive import WebSocketClient
from massive.websocket.models import WebSocketMessage, Feed, Market
from typing import List
import pandas as pd

import config
import db_ops
import edgar_service_v2
import fingrep_service
import fundamentals_service
import polygon_service
import telegram.telegram_service
import utils
from db_ops import get_existing_tickers

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
    # ds = db_ops.fetch_ticker_id_map('SELECT ticker, id FROM shares where id BETWEEN 1 AND 10')
    # print(ds)
    import requests

    # CHANNEL_ID = "-1003939979184"

    # telegram.telegram_service.post_trade_alert(image_path=None, channel_id=CHANNEL_ID, message='Buy', link='https://fingrep.com/quote/AAL?timestamp=2026-05-15&tf=D')

    # fingrep_service.insert_minute_bars_for_ticker('NVDA',2524,datetime.datetime(2026,6,24), datetime.datetime(2026,6,28))

    # fingrep_service.insert_minute_bars_for_ticker('NVDA', 2524,datetime.date(2026,7,5),datetime.date(2026,7,11))
    # ticker_id = {'NKE': 2518, 'BE': 2519, 'CVNY': 2520, 'RIGL': 2521, 'USPH': 2522, 'JCSE': 2523, 'NVDA': 2524, 'DFVX': 2525, 'AVNM': 2526, 'ILCV': 2527, 'IRE': 2528, 'SFLO': 2529, 'NTCT': 2530, 'EFAX': 2531, 'FTRB': 2532, 'MRNY': 2533}
    # fingrep_service.insert_minute_bars_for_date(ticker_id, datetime.date(2026, 7, 10))
    print(fingrep_service.get_splits())
    splits = ['TZA', 'TECS', 'SOXS', 'SFCO', 'MUU', 'LMED', 'KORU', 'GREH', 'GGLS', 'DRIP', 'AMDD', 'AIBD']
    print(splits)
    # data = polygon_service.request_aggregate_daily_bars('GGLS', datetime.date(2026,6,17), 5000)
    # df_aggregated_daily = pd.DataFrame(data['results'])
    #
    # # Prepare for insert
    # df_aggregated_daily['v'] = df_aggregated_daily['v'].astype(int)
    # df_aggregated_daily.drop(['n', 'otc'], axis=1, errors='ignore', inplace=True)
    # df_aggregated_daily.rename(columns={'v': 'volume', 'o': 'open', 'c': 'close', 'h': 'high', 'l': 'low', 'vw': 'vwap', 't': 'date'}, inplace=True)
    # df_aggregated_daily.dropna(subset=['open', 'high', 'low'], inplace=True)
    # df_aggregated_daily['volume'] = df_aggregated_daily['volume'].fillna(0)
    # df_aggregated_daily['date'] = pd.to_datetime(df_aggregated_daily['date'], unit='ms').dt.date
