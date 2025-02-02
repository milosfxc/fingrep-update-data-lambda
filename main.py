import pandas as pd
from datetime import datetime, timezone, timedelta
from sshtunnel import BaseSSHTunnelForwarderError

import config
import db_ops
import edgar
import fingrep_service
from ConnType import DBLocation
from SSHTunnelManager import SSHTunnelManager
from db_ops import get_existing_tickers, get_banned_tickers
from config import logger
def get_stock_data():
    # Get existing tickers, banned tickers and new daily data
    existing_tickers = get_existing_tickers()
    banned_tickers = get_banned_tickers()
    df_grouped_daily = fingrep_service.get_grouped_daily_bars()
    # Data frame for existing tickers
    df_grouped_daily['id'] = df_grouped_daily['T'].map(existing_tickers)
    df_grouped_daily_existing = df_grouped_daily.dropna(subset=['id'])
    # Importing data for existing tickers
    fingrep_service.rename_and_insert_grouped_daily_bars(df_grouped_daily_existing.copy())

    # Data frame for new tickers
    df_grouped_daily_new = df_grouped_daily[df_grouped_daily['id'].isna()]
    df_grouped_daily_new = df_grouped_daily_new[~df_grouped_daily_new['T'].isin(banned_tickers.keys())]
    tickers_list = df_grouped_daily_new['T'].values.tolist()
    counter = 0
    foreign_keys_db = db_ops.get_foreign_keys()
    finviz_df = pd.read_csv('data/finviz_sic.csv')

    for new_ticker in tickers_list:
        if counter == config.LIMIT:
            break
        fingrep_service.get_new_ticker_data_and_insert(new_ticker, finviz_df)

        print(counter)
        counter += 1

    # Update ATR and RSI for existing tickers
    fingrep_service.update_rsi_existing_tickers()

    # Update market breadth
    # if not config.mb_historical:
    #     db_ops.update_market_breadth(get_formatted_utc_date())
    # else:
    #     for i in range(100, 0, -1):
    #         date_str = datetime.utcnow() - timedelta(days=i)
    #         date_str = date_str.strftime("%Y-%m-%d")
    #         db_ops.update_market_breadth(date_str)

    # Stock splits check
    tickers_split = fingrep_service.get_splits()
    if tickers_split:
        existing_tickers_set = set(existing_tickers)
        common_tickers = [ticker for ticker in tickers_split if ticker in existing_tickers_set]
        for ticker in common_tickers:
            ticker_id = existing_tickers[ticker]
            if db_ops.delete_aggregate_bars(ticker_id):
                date_from = datetime.utcnow().replace(tzinfo=timezone.utc).date() - timedelta(days=365 * config.YEARS)
                fingrep_service.get_and_insert_aggregated_bars(ticker, ticker_id, date_from, 5000)
            else:
                logger.error(f"Couldn't delete and reinsert ticker {ticker} for stock split.")

    # Update fundamentals
    data = edgar.get_latest_fillings()
    if data:
        db_ops.insert_latest_fillings(data)
    ciks = db_ops.get_fillings_older_than_four_days()
    if ciks:
        for cik in ciks:
            result = db_ops.get_id_and_ticker_by_cik(cik)
            if result:
                share_id = result.get('share_id')
                ticker = result.get('ticker')
                if share_id and ticker:
                    fingrep_service.get_and_insert_fundamentals(cik=cik, share_id=share_id, ticker=ticker, period='A')
    db_ops.delete_fillings_older_than_four_days()


if __name__ == "__main__":
    if config.db_location == DBLocation.REMOTE:
        try:
            with SSHTunnelManager():
                get_stock_data()
        except BaseSSHTunnelForwarderError as ssh_error:
            logger.error(f"SSH tunnel error occurred: {ssh_error}")
            raise
    else:
        get_stock_data()



