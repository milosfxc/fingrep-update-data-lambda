import concurrent
import time
import aws_service
import clickhouse_service
from config import logger, update_fundamentals, update_existing_tickers_1m_timeframe
import pandas as pd
from datetime import datetime, timezone, timedelta, UTC
import config
import db_ops
import fingrep_service
from db_ops import get_existing_tickers, get_banned_tickers, insert_new_ticker, alter_d_timeframe_triggers
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
        if update_existing_tickers_1m_timeframe:
            fingrep_service.insert_minute_bars_for_date(existing_tickers, get_utc_date(config.days))
        # Update market metrics existing tickers
        if config.market_metrics:
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
        if config.insert_new_tickers:
            if config.multi_threaded:
                with ThreadPoolExecutor(max_workers=config.threads_number) as executor:
                    futures = []
                    for i, new_ticker in enumerate(tickers_list[:config.insert_new_tickers_limit]):
                        future = executor.submit(fingrep_service.get_new_ticker_data_and_insert,new_ticker, finviz_df)
                        futures.append(future)
                        time.sleep(config.thread_delay)
                    # Wait for all tasks to complete
                    for i, future in enumerate(concurrent.futures.as_completed(futures)):
                        try:
                            future.result()  # This waits for completion and re-raises exceptions
                            print(f"Completed {i + 1}/{config.insert_new_tickers_limit}")
                        except Exception as e:
                            print(f"Task {i + 1} failed: {e}")
            else:
                for new_ticker in tickers_list:
                    if counter == config.insert_new_tickers_limit:
                        break
                    fingrep_service.get_new_ticker_data_and_insert(new_ticker, finviz_df)
                    print(f'Inserted new ticker {counter}')
                    counter += 1
                    if config.s3_upload and counter % config.s3_upload_limit == 0:
                        aws_service.update_s3_bucket()
    # Update market breadth
    if not config.mb_historical:
        db_ops.update_market_breadth(get_utc_date(days=config.days))
    else:
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
            if config.market_metrics:  # todo remove this line when you implement the clickhouse market metrics solution.
                fingrep_service.update_market_metrics_shares_outstanding(ticker, ticker_id)
    # Update fundamentals
    if update_fundamentals:
        update_fundamentals()
    # # Add missing shares to the remote
    # if config.missing_remote_shares:
    #     [aws_service.add_share_id(i, 'new') for i in config.missing_remote_shares]
    # # Upload data to S3
    # if config.s3_upload:
    #     aws_service.update_s3_bucket()



if __name__ == "__main__":
    try:
        get_stock_data()
    except Exception as e:
        logger.error(f"get_stock_market_data error occurred: {e}")
    finally:
        alter_d_timeframe_triggers(full='DISABLE', compact='ENABLE')


