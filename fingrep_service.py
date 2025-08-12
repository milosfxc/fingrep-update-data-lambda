import sys
from datetime import datetime,date, timezone, timedelta
import inspect

import pandas as pd
import requests
import config
import db_ops
import finviz
from fundamentals_service import get_company_fundamentals

import polygon
import utils
from db_ops import upsert_dataframe, foreign_keys_cache
from edgar_api import get_trading_info
from ta_utils import rsi_tv_new_tickers, rsi_tv_existing_tickers
from utils import get_utc_date

pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', None)  # To allow the console to use the full width
pd.set_option("future.no_silent_downcasting", True)
from config import logger


def call_and_update_market_breadth(date):
    db_ops.update_market_breadth(date)


def get_grouped_daily_bars(date_str: str) -> pd.DataFrame:
        data = polygon.request_grouped_daily_bars(date=date_str)
        if data['resultsCount'] == 0:
            return pd.DataFrame()
        df = pd.DataFrame(data['results'])
        # Checks resultsCount and the actual results array length
        row_count = len(df)
        if row_count < 9000:
            logger.critical(f"Request_grouped_daily_bars has returned {row_count} and expected at least 9000. Exiting the script.",stack_info=True)
            sys.exit(1)
        # Removes rows that contain at least one NaN OHLC value
        tickers_with_nan = df.loc[df[['o', 'h', 'l', 'c']].isna().any(axis=1), 'T'].tolist()
        if tickers_with_nan:
            logger.warning(f": The following tickers had at least one NaN OHLC value on the date {get_utc_date(days=config.DAYS)}: ",
                           tickers_with_nan)
        # Volume column conversion to integer
        df['v'] = df['v'].astype(int)
        return df


def get_and_insert_aggregated_bars(ticker, ticker_id, date_from, limit):

    data = polygon.request_aggregate_daily_bars(ticker, date_from, limit)
    df_aggregated_daily = pd.DataFrame(data['results'])

    # Prepare for insert
    df_aggregated_daily['v'] = df_aggregated_daily['v'].astype(int)
    df_aggregated_daily['share_id'] = ticker_id
    df_aggregated_daily.drop(['n', 'otc'], axis=1, errors='ignore', inplace=True)
    df_aggregated_daily.rename(columns={'v': 'volume', 'o': 'open', 'c': 'close', 'h': 'high', 'l': 'low',
                                        'vw': 'vwap', 't': 'date'},
                               inplace=True)
    df_aggregated_daily.dropna(subset=['open', 'high', 'low'], inplace=True)
    df_aggregated_daily['volume'] = df_aggregated_daily['volume'].fillna(0)
    df_aggregated_daily['date'] = pd.to_datetime(df_aggregated_daily['date'], unit='ms').dt.date
    df_aggregated_daily['rsi'] = rsi_tv_new_tickers(df_aggregated_daily.copy())
    df_aggregated_daily[utils.magnified_columns_new] = df_aggregated_daily[utils.magnified_columns_new] * 10000

    # Insert into database
    db_ops.upsert_dataframe_v2(df_aggregated_daily, 'd_timeframe')


def insert_grouped_daily_bars(df):
    df['date'] = pd.to_datetime(df['t'], unit='ms').dt.date
    df = df.drop(['T', 'n', 't'], axis=1)
    df['id'] = df['id'].astype(int)
    df.rename(
        columns={'v': 'volume', 'o': 'open', 'c': 'close', 'h': 'high', 'l': 'low', 'id': 'share_id', 'vw': 'vwap'},
        inplace=True)
    df[utils.magnified_columns_existing] = df[utils.magnified_columns_existing] * 10000
    db_ops.upsert_dataframe_v2(df, 'd_timeframe')


def get_new_ticker_data_and_insert(ticker, finviz_df):
    method_name = inspect.currentframe().f_code.co_name
    try:
        ticker_data = polygon.request_ticker_details_v3(ticker)
    except requests.RequestException:
        logger.warning(f"{method_name} - Couldn't obtain ticker details.")
        return

    finviz_ticker_df = finviz_df.query("ticker == @ticker")
    if not finviz_ticker_df.empty:
        finviz_data = {
            'sector': finviz_ticker_df['sector'].values[0],
            'industry': finviz_ticker_df['industry'].values[0],
            'country': finviz_ticker_df['country'].values[0]
        }
    else:
        finviz_data = finviz.get_sic(ticker)
    shares_data, shares_info_data = extract_ticker_details_v3(ticker_data, finviz_data)
    # Check if ticker type is allowed
    if shares_data.get("share_type_id") is None:
        return
    elif shares_data.get("share_type_id") not in utils.allowed_share_type_ids:
        db_ops.insert_banned_ticker(ticker)
        return
    ticker_id = db_ops.insert_new_ticker(shares_data, shares_info_data)
    # Starter plan required for 2+ years historical data
    date_from = datetime.utcnow().replace(tzinfo=timezone.utc).date() - timedelta(days=365 * config.YEARS)
    get_and_insert_aggregated_bars(ticker, ticker_id, date_from, 5000)
    # Fundamental data and trade info
    cik = shares_info_data.get('cik')
    if config.insert_fundamentals and cik is not None and ticker_id is not None and shares_data.get('share_type_id') not in(6, 8):
        get_company_fundamentals(ticker, ticker_id, config.report_start_date)


# Separates data for shares and share_info tables
def extract_ticker_details_v3(ticker_details, finviz_data):

    ticker_data = {
        'ticker': ticker_details.get('ticker'),
        'cik': ticker_details.get('cik'),
        'name': utils.remove_stock_suffix(ticker_details.get('name')),
        'exchange_id': db_ops.get_cached_foreign_keys()['exchanges'].get(ticker_details.get('primary_exchange')),
        'homepage_url': ticker_details.get('homepage_url'),
        'ipo_date': ticker_details.get('list_date'),
        'share_type_id': db_ops.get_cached_foreign_keys()['share_types'].get(ticker_details.get('type')),
        'composite_figi': ticker_details.get('composite_figi'),
        'shares_outstanding': ticker_details.get('share_class_shares_outstanding'),
        'weighted_shares_outstanding': ticker_details.get('weighted_shares_outstanding'),
        'sector_id': db_ops.get_cached_foreign_keys()['sectors'].get(finviz_data.get('sector')),
        'industry_id': db_ops.get_cached_foreign_keys()['industries'].get(finviz_data.get('industry')),
        'country_id': db_ops.get_cached_foreign_keys()['countries'].get(finviz_data.get('country'))
    }

    shares = {key: value for key, value in ticker_data.items() if
              key not in ['weighted_shares_outstanding', 'ipo_date', 'shares_outstanding', 'cik', 'homepage_url', 'composite_figi']}
    shares_info = {key: ticker_data[key] for key in
                   ['weighted_shares_outstanding', 'ipo_date', 'shares_outstanding', 'cik', 'homepage_url', 'composite_figi']}

    return shares, shares_info


def update_rsi_existing_tickers():
    df_last_100 = db_ops.get_last_100()
    df_last_100.sort_values(by='date', ascending=True, inplace=True)
    df_last_100['rsi'] = df_last_100.groupby('share_id', as_index=False).apply(
        lambda group: rsi_tv_existing_tickers(group), include_groups=False).reset_index(level=0, drop=True)
    utc_now = datetime.utcnow().replace(tzinfo=timezone.utc).date() - timedelta(days=config.DAYS)
    df_last_100 = df_last_100.query("date == @utc_now")
    df_last_100 = df_last_100.drop(columns=['close', 'high', 'low'])
    df_last_100.dropna(subset=['rsi'], inplace=True)
    update_data = [(row['rsi'], row['share_id'], row['date'])
                   for index, row in df_last_100.iterrows()]
    # Update db
    db_ops.update_rsi(update_data)


def get_splits():
    try:
        res = polygon.request_splits()
        arr = [entry['ticker'] for entry in res]
        return arr
    except TypeError as e:
        logger.error(f"get_splits - TypeError {e}")
    except requests.RequestException as e:
        logger.error(f"get_splits - RequestException {e}")


def get_prev_grouped_daily_bars():
    for i in range(1,10):
        prev_date = get_utc_date(days=config.DAYS + i, as_str=False)
        if prev_date.weekday() in (5, 6):
            continue
        df = get_grouped_daily_bars(date_str=prev_date.strftime("%Y-%m-%d"))
        if not df.empty:
            return df
    logger.critical(f"Couldn't obtain previous day grouped daily data. Exiting the script.", exc_info=True)
    sys.exit(1)

def get_all_tickers(date_str: str):
    return polygon.request_all_tickers(date=date_str)

