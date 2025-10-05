import concurrent
import time

import aws_service
from config import logger, update_fundamentals
import pandas as pd
from datetime import datetime, timezone, timedelta
from sshtunnel import BaseSSHTunnelForwarderError
import config
import db_ops
import fingrep_service
from ConnType import DBLocation
from SSHTunnelManager import SSHTunnelManager
from db_ops import get_existing_tickers, get_banned_tickers
from utils import get_utc_date
from fundamentals_service import update_fundamentals
from concurrent.futures import ThreadPoolExecutor


def get_stock_data():
    # Get existing tickers, banned tickers and new daily data
    existing_tickers = get_existing_tickers()
    banned_tickers = get_banned_tickers()
    df_grouped_daily = fingrep_service.get_grouped_daily_bars(date_str=get_utc_date(config.days))
    # Data frame for existing tickers
    df_grouped_daily['id'] = df_grouped_daily['T'].map(existing_tickers)
    df_grouped_daily_existing = df_grouped_daily.dropna(subset=['id'])
    # Importing data for existing tickers
    fingrep_service.insert_grouped_daily_bars(df_grouped_daily_existing.copy())
    # Data frame for new tickers
    df_grouped_daily_new = df_grouped_daily[df_grouped_daily['id'].isna()]
    df_grouped_daily_new = df_grouped_daily_new[~df_grouped_daily_new['T'].isin(banned_tickers.keys())]
    tickers_list = df_grouped_daily_new['T'].values.tolist()
    counter = 0
    foreign_keys_db = db_ops.get_foreign_keys()
    finviz_df = pd.read_csv('data/finviz_sic.csv')
    # Get new tickers in threaded environment
    if config.multi_threaded:
        with ThreadPoolExecutor(max_workers=config.threads_number) as executor:
            futures = []
            for i, new_ticker in enumerate(tickers_list[:config.limit]):
                future = executor.submit(fingrep_service.get_new_ticker_data_and_insert,new_ticker, finviz_df)
                futures.append(future)
                time.sleep(config.thread_delay)
            # Wait for all tasks to complete
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                try:
                    future.result()  # This waits for completion and re-raises exceptions
                    print(f"Completed {i + 1}/{config.limit}")
                except Exception as e:
                    print(f"Task {i + 1} failed: {e}")
    else:
        for new_ticker in tickers_list:
            if counter == config.limit:
                break
            fingrep_service.get_new_ticker_data_and_insert(new_ticker, finviz_df)
            print(counter)
            counter += 1
    # Update RSI for existing tickers
    fingrep_service.update_rsi_existing_tickers()

    # Update market breadth
    if not config.mb_historical:
        db_ops.update_market_breadth(get_utc_date(days=config.days))
    else:
        for i in range(100, 0, -1):
            date_str = datetime.utcnow() - timedelta(days=i)
            date_str = date_str.strftime("%Y-%m-%d")
            db_ops.update_market_breadth(date_str) # todo s3 bucket

    # Stock splits check
    tickers_split = fingrep_service.get_splits()
    if tickers_split:
        existing_tickers_set = set(existing_tickers)
        common_tickers = [ticker for ticker in tickers_split if ticker in existing_tickers_set]
        for ticker in common_tickers:
            ticker_id = existing_tickers[ticker]
            if db_ops.delete_aggregate_bars(ticker_id):
                date_from = datetime.utcnow().replace(tzinfo=timezone.utc).date() - timedelta(days=365 * config.years)
                fingrep_service.get_and_insert_aggregated_bars(ticker, ticker_id, date_from, 5000)
                aws.add_share_id(ticker_id, 'split')
            else:
                logger.error(f"Couldn't delete and reinsert ticker {ticker} for stock split.")

    # Update fundamentals
    if update_fundamentals:
        update_fundamentals()
    # Upload data to S3
    if config.s3_upload:
        aws.update_s3_bucket()


if __name__ == "__main__":
    start_time = time.perf_counter()
    if config.db_location == DBLocation.REMOTE:
        try:
            with SSHTunnelManager():
                get_stock_data()
        except BaseSSHTunnelForwarderError as ssh_error:
            logger.error(f"SSH tunnel error occurred: {ssh_error}")
            raise
    else:
        get_stock_data()
    end_time = time.perf_counter()
    execution_time = end_time - start_time
    print(f"Execution time {execution_time} seconds.")