import concurrent
import time
import aws_service
import clickhouse_service
from config import logger, ENABLE_UPDATE_FUNDAMENTALS, ENABLE_UPDATE_EXISTING_TICKERS_1M_TIMEFRAME
import pandas as pd
from datetime import datetime, timezone, timedelta, UTC
import config
import db_ops
import fingrep_service
from db_ops import get_existing_tickers, get_banned_tickers, alter_d_timeframe_triggers
from fingrep_service import fetch_and_update_market_metrics
from utils import get_utc_date
from fundamentals_service import update_fundamentals
from concurrent.futures import ThreadPoolExecutor


def get_stock_data():
    print(get_utc_date(days=config.days))
    # Get existing tickers, banned tickers and new daily data
    existing_tickers = get_existing_tickers()
    banned_tickers = get_banned_tickers()
    df_grouped_daily = fingrep_service.get_grouped_daily_bars(date_str=get_utc_date(config.days))
    if not df_grouped_daily.empty:
        # Data frame for existing tickers
        df_grouped_daily['id'] = df_grouped_daily['T'].map(existing_tickers)
        df_grouped_daily_existing = df_grouped_daily.dropna(subset=['id'])
        # Importing data for existing tickers
        startt_time = time.perf_counter()
        fingrep_service.insert_grouped_daily_bars(df_grouped_daily_existing.copy())
        print(time.perf_counter() - startt_time)
        # Update 1m timeframe for existing tickers
        if ENABLE_UPDATE_EXISTING_TICKERS_1M_TIMEFRAME:
            fingrep_service.insert_minute_bars_for_date(existing_tickers, get_utc_date(config.days,as_str=False), config.ENABLE_FLAT_FILE_1M_TIMEFRAME)
        # Update market metrics existing tickers
        if config.ENABLE_MARKET_METRICS:
            fetch_and_update_market_metrics(existing_tickers)
        # Data frame for new tickers
        df_grouped_daily_new = df_grouped_daily[df_grouped_daily['id'].isna()]
        df_grouped_daily_new = df_grouped_daily_new[~df_grouped_daily_new['T'].isin(banned_tickers.keys())]
        tickers_list = df_grouped_daily_new['T'].values.tolist()
        print(f"{len(tickers_list)} tickers to insert")
        counter = 0
        foreign_keys_db = db_ops.get_foreign_keys()
        finviz_df = pd.read_csv('data/finviz_sic.csv')
        # Get new tickers in threaded environment
        if config.ENABLE_INSERT_NEW_TICKERS:
            if config.multi_threaded:
                with ThreadPoolExecutor(max_workers=config.threads_number) as executor:
                    futures = []
                    for i, new_ticker in enumerate(tickers_list[:config.INSERT_NEW_TICKERS_LIMIT]):
                        future = executor.submit(fingrep_service.get_new_ticker_data_and_insert,new_ticker, finviz_df)
                        futures.append(future)
                        time.sleep(config.thread_delay)
                    # Wait for all tasks to complete
                    for i, future in enumerate(concurrent.futures.as_completed(futures)):
                        try:
                            future.result()  # This waits for completion and re-raises exceptions
                            print(f"Completed {i + 1}/{config.INSERT_NEW_TICKERS_LIMIT}")
                        except Exception as e:
                            print(f"Task {i + 1} failed: {e}")
            else:
                for new_ticker in tickers_list:
                    if counter == config.INSERT_NEW_TICKERS_LIMIT:
                        break
                    fingrep_service.get_new_ticker_data_and_insert(new_ticker, finviz_df)
                    print(f'Inserted new ticker {counter}')
                    counter += 1
    # Update market breadth
    if config.ENABLE_MB and not config.ENABLE_MB_HISTORICAL:
        db_ops.update_market_breadth(get_utc_date(days=config.days))
    elif config.ENABLE_MB:
        for i in range(100, 0, -1):
            date_str = datetime.now(timezone.utc) - timedelta(days=i)
            date_str = date_str.strftime("%Y-%m-%d")
            db_ops.update_market_breadth(date_str) # todo s3 bucket
    # Stock splits
    for ticker in fingrep_service.get_splits():
        if ticker_id := existing_tickers.get(ticker):
            date_end = datetime.now(UTC).date()
            date_start = date_end  - timedelta(days=365 * config.years)
            fingrep_service.get_and_insert_aggregated_bars(ticker, ticker_id, date_start, 5000)
            fingrep_service.insert_minute_bars_for_ticker(ticker, ticker_id, date_end - timedelta(days=config.days_1m), date_end)
            if config.ENABLE_MARKET_METRICS:  # todo remove this line when you implement the clickhouse market metrics solution.
                fingrep_service.update_market_metrics_shares_outstanding(ticker, ticker_id)
    # Update fundamentals
    if ENABLE_UPDATE_FUNDAMENTALS:
        update_fundamentals()
    # Refresh data from postgresql
    clickhouse_service.refresh_shares_from_postgres()
    clickhouse_service.refresh_shares_info_from_postgres()



if __name__ == "__main__":

    get_stock_data()



