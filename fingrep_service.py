from datetime import datetime,date, timezone, timedelta
import inspect
import time
from xxsubtype import bench

import pandas as pd
import requests

import constant
import db_ops
import edgar
import finviz
import polygon
import utils
from db_ops import upsert_dataframe, get_foreign_keys, foreign_keys_cache
from edgar import get_trading_info
import logging
from fmp import get_fundamentals_v2
from ta_utils import rsi_tv_new_tickers, rsi_tv_existing_tickers
pd.set_option("future.no_silent_downcasting", True)
logger = logging.getLogger(__name__)


def get_and_insert_fundamentals(cik: str, share_id: int, ticker: str, period: str,
                                date: datetime = None):
    method_name = inspect.currentframe().f_code.co_name
    for statement in ['income_statement', 'cash_flow', 'balance_sheet']:
        response_json = get_fundamentals_v2(statement=statement, ticker=ticker, period='annual')
        if response_json is None or 'Error Message' in response_json:
            time.sleep(3)
            response_json = get_fundamentals_v2(statement=statement, ticker=ticker, period=period)
            if response_json is None or 'Error Message' in response_json:
                logger.error(f"{get_and_insert_fundamentals.__name__}: No data returned for {statement} "
                             f"and ticker {ticker}. The response: {response_json}.")
                break
        try:
            df = pd.DataFrame(response_json)
            if date is not None:
                date_str = date.strftime('%Y-%m-%d')
                df = df.query('date > @date_str')

            if 'cik' not in df.columns or df.iloc[0]['cik'] == '0000000000':
                edgar_ticker = edgar.get_ticker_by_cik(cik)
                if edgar_ticker and edgar_ticker != ticker:
                    logger.warning(f"{method_name} - CIK {cik} mismatch for ticker {ticker}")
                    break
            elif df.iloc[0]['cik'] != cik.zfill(10):
                logger.warning(f"{method_name} - CIK {cik} mismatch for ticker {ticker}")
                break

            currency_mapping = db_ops.get_foreign_keys()['currencies']
            df['currency_id'] = df['reportedCurrency'].replace(currency_mapping)
            df.loc[:, ['period', 'share_id']] = 'A', share_id
            df = df[utils.pg_tables.get(statement).keys()]
            df = df.rename(columns=utils.pg_tables.get(statement))
            df.sort_values(by=['date'], inplace=True, ascending=True)
            upsert_dataframe(df, statement)
        except Exception as e:
            logger.error(f"{method_name} - Error preparing for insert fundamentals for ticker {ticker}: {e}")


def get_and_insert_trading_info(cik: str, share_id: int, date: datetime.date):
    method_name = inspect.currentframe().f_code.co_name

    try:
        df = get_trading_info(cik=cik, date=date)
        if df is not None:
            df.reset_index(inplace=True)
            df.rename(columns={'end': 'date'}, inplace=True)
            df.loc[:, ['share_id']] = share_id
            upsert_dataframe(df, 'trade_info')
    except Exception as e:
        logger.error(
            f"{method_name}: Error occurred while trying to rename and reindex data frame for upsert into database for CIK {cik} "
            f"and share ID {share_id}: {e}")


def call_and_update_market_breadth(date):
    db_ops.update_market_breadth(date)


def get_grouped_daily_bars():

        data = polygon.request_grouped_daily_bars()
        df = pd.DataFrame(data['results'])
        # Checks resultsCount and the actual results array length
        method_name = inspect.currentframe().f_code.co_name
        row_count = len(df)
        if row_count < 8000:
            logger.warning(f"{method_name}: request_grouped_daily_bars has returned {row_count}.")
        # Removes rows that contain at least one NaN OHLC value
        tickers_with_nan = df.loc[df[['o', 'h', 'l', 'c']].isna().any(axis=1), 'T'].tolist()
        if tickers_with_nan:
            logger.warning(f"{method_name}: The following tickers had at least one NaN OHLC value on the date ", datetime.utcnow(), ":",
                  tickers_with_nan)
        # Volume column conversion to integer
        df['v'] = df['v'].astype(int)
        return df


def get_and_insert_aggregated_bars(ticker, ticker_id, date_from, limit):
    method_name = inspect.currentframe().f_code.co_name

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

    # Insert into database
    db_ops.upsert_dataframe_v2(df_aggregated_daily, 'd_timeframe', method_name)


def rename_and_insert_grouped_daily_bars(df):
    method_name = inspect.currentframe().f_code.co_name
    df['date'] = pd.to_datetime(df['t'], unit='ms').dt.date
    df = df.drop(['T', 'n', 't'], axis=1)
    df['id'] = df['id'].astype(int)
    df.rename(
        columns={'v': 'volume', 'o': 'open', 'c': 'close', 'h': 'high', 'l': 'low', 'id': 'share_id', 'vw': 'vwap'},
        inplace=True)
    db_ops.upsert_dataframe_v2(df, 'd_timeframe', method_name)


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
    ticker_id = db_ops.insert_new_ticker_v2(shares_data, shares_info_data)
    # Starter plan required for 2+ years historical data
    date_from = datetime.utcnow().replace(tzinfo=timezone.utc).date() - timedelta(days=365 * constant.YEARS)
    get_and_insert_aggregated_bars(ticker, ticker_id, date_from, 5000)
    # Fundamental data and trade info
    cik = shares_info_data.get('cik')
    if cik is not None and ticker_id is not None:
        get_and_insert_trading_info(cik=cik, share_id=ticker_id, date=date(2019, 12, 30))
        get_and_insert_fundamentals(cik=cik, share_id=ticker_id, ticker=ticker, period='annual')


# Separates data for shares and share_info tables
def extract_ticker_details_v3(ticker_details, finviz_data):
    foreign_keys = db_ops.get_foreign_keys()
    ticker_data = {
        'ticker': ticker_details.get('ticker'),
        'cik': ticker_details.get('cik'),
        'name': utils.remove_stock_suffix(ticker_details.get('name')),
        'exchange_id': foreign_keys['exchanges'].get(ticker_details.get('primary_exchange')),
        'currency_id': foreign_keys['currencies'].get(ticker_details.get('currency_name').upper()),
        'homepage_url': ticker_details.get('homepage_url'),
        'ipo_date': ticker_details.get('list_date'),
        'share_type_id': foreign_keys['share_types'].get(ticker_details.get('type')),
        'shares_outstanding': ticker_details.get('share_class_shares_outstanding'),
        'weighted_shares_outstanding': ticker_details.get('weighted_shares_outstanding'),
        'sector_id': foreign_keys['sectors'].get(finviz_data.get('sector')),
        'industry_id': foreign_keys['industries'].get(finviz_data.get('industry')),
        'country_id': foreign_keys['countries'].get(finviz_data.get('country'))
    }

    shares = {key: value for key, value in ticker_data.items() if
              key not in ['weighted_shares_outstanding', 'ipo_date', 'shares_outstanding', 'cik', 'homepage_url']}
    shares_info = {key: ticker_data[key] for key in
                   ['weighted_shares_outstanding', 'ipo_date', 'shares_outstanding', 'cik', 'homepage_url']}

    return shares, shares_info


def update_rsi_existing_tickers():
    df_last_100 = db_ops.get_last_100()
    df_last_100.sort_values(by='date', ascending=True, inplace=True)
    df_last_100['rsi'] = df_last_100.groupby('share_id', as_index=False).apply(
        lambda group: rsi_tv_existing_tickers(group), include_groups=False).reset_index(level=0, drop=True)
    utc_now = datetime.utcnow().replace(tzinfo=timezone.utc).date() - timedelta(days=constant.DAYS)
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


