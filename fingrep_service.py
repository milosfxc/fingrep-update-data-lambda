import sys
from datetime import datetime, date, timezone, timedelta, time
import inspect
from typing import List
import time
from cffi.cffi_opcode import PRIM_INT
from massive.websocket.models import WebSocketMessage, Feed, Market

import pandas as pd
import requests
import aws_service
import config
import db_ops
import finviz
from fundamentals_service import get_company_fundamentals
import polygon_service
import utils
from ta_utils import rsi_tv_new_tickers, rsi_tv_existing_tickers
from utils import get_utc_date, is_number
from db_ops_v2 import upsert_data_smart
pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', None)  # To allow the console to use the full width
pd.set_option("future.no_silent_downcasting", True)
from config import logger


def call_and_update_market_breadth(date):
    db_ops.update_market_breadth(date)


def get_grouped_daily_bars(date_str: str) -> pd.DataFrame:
        data = polygon_service.request_grouped_daily_bars(date=date_str)
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
            logger.warning(f": The following tickers had at least one NaN OHLC value on the date {get_utc_date(days=config.days)}: ",
                           tickers_with_nan)
        # Volume column conversion to integer
        df['v'] = df['v'].astype(int)
        return df


def get_and_insert_aggregated_bars(ticker, ticker_id, date_from, limit):

    data = polygon_service.request_aggregate_daily_bars(ticker, date_from, limit)
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

def insert_minute_bars_for_date(tickers: dict[str,int], date_obj: date):
    us_market_open = utils.us_market_open_utc(date_obj)
    us_premarket_open = us_market_open - timedelta(hours=5.5)
    us_market_close = us_market_open + timedelta(hours=6.5)
    us_aftermarket_close = us_market_close + timedelta(hours=4)
    date_start = str(int(us_premarket_open.timestamp() * 1000))
    date_end = str(int(us_aftermarket_close.timestamp() * 1000))
    insert_list = []
    counter = 0
    for ticker, share_id in tickers.items():
        counter += 1
        print(counter)
        if counter == 101: break # todo delete this line later
        data = polygon_service.request_aggregate_bars(ticker=ticker, timeframe="minute", multiplier=1, date_start=date_start, date_end=date_end, limit=50000)
        if data is None or 'results' not in data:
            logger.warning(f"No 1m data found for ticker {ticker}")
            continue
        ohlcv_list = data['results']
        for ohlcv_dict in ohlcv_list:
            bar_datetime = datetime.fromtimestamp(ohlcv_dict['t'] / 1000, timezone.utc)
            insert_dict = {
                'datetime': bar_datetime,
                'abs_atr': None,
                'avg_volume': None,
                'close': int(ohlcv_dict['c'] * 10_000),
                'convergence2': None,
                'convergence3': None,
                'high': int(ohlcv_dict['h'] * 10_000),
                'low': int(ohlcv_dict['l'] * 10_000),
                'open': int(ohlcv_dict['o'] * 10_000),
                'rel_volume': None,
                'session': 0 if bar_datetime.time() < us_market_open.time() else 1 if bar_datetime.time() < us_market_close.time() else 2,
                'sma10': None,
                'volume': int(ohlcv_dict['v'] * 10_000),
                'vwap': int(ohlcv_dict['vw'] * 10_000),
                'share_id': share_id,
                'time': bar_datetime.hour * 100 + bar_datetime.minute
            }
            insert_list.append(insert_dict)
        if counter % 50 == 0:
            start_time = time.perf_counter()
            upsert_data_smart(insert_list,'timeframe_1m',{'share_id','datetime'})
            print(time.perf_counter() - start_time)
            insert_list = []
    if insert_list:
        upsert_data_smart(insert_list,'timeframe_1m',{'share_id','datetime'})

def insert_minute_bars_for_ticker(ticker: str, share_id: int, date_start: date, date_end: date):
    date_iter = date_start
    insert_list = []
    while date_iter <= date_end:
        us_market_open = utils.us_market_open_utc(date_iter)
        us_premarket_open = us_market_open - timedelta(hours=5.5)
        us_market_close = us_market_open + timedelta(hours=6.5)
        us_aftermarket_close = us_market_close + timedelta(hours=4)
        request_date_start = str(int(us_premarket_open.timestamp() * 1000))
        request_date_end = str(int(us_aftermarket_close.timestamp() * 1000))
        data = polygon_service.request_aggregate_bars(ticker=ticker, timeframe="minute", multiplier=1, date_start=request_date_start, date_end=request_date_end, limit=50000)
        ohlcv_list = data['results']
        for ohlcv_dict in ohlcv_list:
            bar_datetime = datetime.fromtimestamp(ohlcv_dict['t'] / 1000, timezone.utc)
            insert_dict = {
                'datetime': bar_datetime,
                'abs_atr': None,
                'avg_volume': None,
                'close': int(ohlcv_dict['c'] * 10_000),
                'convergence2': None,
                'convergence3': None,
                'high': int(ohlcv_dict['h'] * 10_000),
                'low': int(ohlcv_dict['l'] * 10_000),
                'open': int(ohlcv_dict['o'] * 10_000),
                'rel_volume': None,
                'session': 0 if bar_datetime.time() < us_market_open.time() else 1 if bar_datetime.time() < us_market_close.time() else 2,
                'sma10': None,
                'volume': int(ohlcv_dict['v'] * 10_000),
                'vwap': int(ohlcv_dict['vw'] * 10_000),
                'share_id': share_id
            }
            insert_list.append(insert_dict)
        date_iter = date_iter + timedelta(days=1)
    upsert_data_smart(insert_list,'timeframe_1m',{'share_id','datetime'})


def get_new_ticker_data_and_insert(ticker, finviz_df):
    method_name = inspect.currentframe().f_code.co_name
    try:
        ticker_data = polygon_service.request_ticker_details_v3(ticker)
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
    # Insert ticker
    ticker_id = db_ops.insert_new_ticker(shares_data, shares_info_data)
    # Append to ids for s3
    aws_service.add_share_id(ticker_id, 'new')
    # Starter plan required for 2+ years historical data
    date_from = datetime.utcnow().replace(tzinfo=timezone.utc).date() - timedelta(days=365 * config.years)
    # Daily timeframe data
    get_and_insert_aggregated_bars(ticker, ticker_id, date_from, 5000)
    # 1m timeframe data
    if config.insert_new_tickers_1m_timeframe:
        insert_minute_bars_for_ticker(ticker, ticker_id, get_utc_date(config.days + config.days_1m), get_utc_date(config.days))
    # Update market metrics
    fetch_and_insert_market_metrics(ticker, ticker_id)
    # Fundamental data and trade info
    cik = shares_info_data.get('cik')
    if config.insert_fundamentals and cik is not None and ticker_id is not None and shares_data.get('share_type_id') not in(6, 8):
        get_company_fundamentals(ticker, ticker_id, config.report_start_date)


# Separates data for shares and share_info tables
def extract_ticker_details_v3(ticker_details, finviz_data):
    shares_outstanding = ticker_details.get('share_class_shares_outstanding')
    weighted_shares_outstanding = ticker_details.get('weighted_shares_outstanding')
    ticker_data = {
        'ticker': ticker_details.get('ticker'),
        'cik': ticker_details.get('cik'),
        'name': utils.remove_stock_suffix(ticker_details.get('name')),
        'exchange_id': db_ops.get_cached_foreign_keys()['exchanges'].get(ticker_details.get('primary_exchange')),
        'homepage_url': ticker_details.get('homepage_url'),
        'ipo_date': ticker_details.get('list_date'),
        'share_type_id': db_ops.get_cached_foreign_keys()['share_types'].get(ticker_details.get('type')),
        'composite_figi': ticker_details.get('composite_figi'),
        'shares_outstanding': shares_outstanding * config.scale_factor if is_number(shares_outstanding) else None,
        'weighted_shares_outstanding': weighted_shares_outstanding * config.scale_factor if is_number(weighted_shares_outstanding) else None,
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
    utc_now = datetime.utcnow().replace(tzinfo=timezone.utc).date() - timedelta(days=config.days)
    df_last_100 = df_last_100.query("date == @utc_now")
    df_last_100 = df_last_100.drop(columns=['close', 'high', 'low'])
    df_last_100.dropna(subset=['rsi'], inplace=True)
    update_data = [(row['rsi'], row['share_id'], row['date'])
                   for index, row in df_last_100.iterrows()]
    # Update db
    db_ops.update_rsi(update_data)


def get_splits():
    try:
        res = polygon_service.request_splits()
        arr = [entry['ticker'] for entry in res]
        return arr
    except TypeError as e:
        logger.error(f"get_splits - TypeError {e}")
    except requests.RequestException as e:
        logger.error(f"get_splits - RequestException {e}")


def get_prev_grouped_daily_bars():
    for i in range(1,10):
        prev_date = get_utc_date(days=config.days + i, as_str=False)
        if prev_date.weekday() in (5, 6):
            continue
        df = get_grouped_daily_bars(date_str=prev_date.strftime("%Y-%m-%d"))
        if not df.empty:
            return df
    logger.critical(f"Couldn't obtain previous day grouped daily data. Exiting the script.", exc_info=True)
    sys.exit(1)

def get_all_tickers(date_str: str):
    return polygon_service.request_all_tickers(date=date_str)

def update_market_metrics_shares_outstanding(ticker:str, ticker_id:int):
    try:
        shares_outstanding = None
        ticker_details = polygon_service.request_ticker_details_v3(ticker)
        if 'share_class_shares_outstanding' in ticker_details:
            shares_outstanding = ticker_details['share_class_shares_outstanding']
        elif 'weighted_shares_outstanding' in ticker_details:
            shares_outstanding = ticker_details['weighted_shares_outstanding']

        if shares_outstanding:
            db_ops.upsert_statement_v2({'share_id': ticker_id, 'date': utils.get_utc_date(config.days,True),'shares_outstanding': shares_outstanding},
                                       'market_metrics',
                                       ['share_id', 'date'])
    except Exception as e:
        logger.error(f"update_market_metrics_shares_outstanding couldn't find shares outstanding for ticker/share_id {ticker}/{ticker_id}: {e}")


def fetch_and_insert_market_metrics(ticker:str, ticker_id:int):
    try:
        # Short interest
        short_interest = polygon_service.request_short_interest(tickers=[ticker], date=config.date_from, date_operator='.gte')
        if short_interest:
            si_list = utils.safe_filter_dict_keys(
                data=short_interest,
                keys_to_keep= {'settlement_date', 'short_interest', 'avg_daily_volume', 'days_to_cover'},
                keys_to_add= {'share_id': ticker_id},
                rename= {'settlement_date': 'date', 'avg_daily_volume': 'avg_f_volume', 'days_to_cover': 'short_interest_ratio'}
            )
            upsert_data_smart(si_list,'market_metrics', {'share_id', 'date'})
        # Short volume
        short_volume = polygon_service.request_short_volume(tickers=[ticker], date=config.date_from, date_operator='.gte')
        if short_volume:
            sv_list = utils.safe_filter_dict_keys(
                data=short_volume,
                keys_to_keep={'date', 'short_volume', 'total_volume', 'short_volume_ratio', 'exempt_volume', 'non_exempt_volume'},
                keys_to_add={'share_id': ticker_id},
                rename={'total_volume': 'f_volume'}
            )
            upsert_data_smart(sv_list,'market_metrics', {'share_id', 'date'})

    except Exception as e:
        logger.error(f"insert_market_metrics couldn't insert market metrics for ticker/share_id {ticker}/{ticker_id}: {e}")


def fetch_and_update_market_metrics(ticker_id_map:dict):
    try:
        tickers = list(ticker_id_map.keys())
        # Short volume
        for i in range(0, len(tickers), 500):
            short_volumes = polygon_service.request_short_volume(tickers=tickers[i:i+500], date=utils.get_utc_date(config.days))
            if short_volumes:
                sv_list = []
                for short_volume in short_volumes:
                    sv_list_item = utils.safe_filter_dict_keys(
                        data=[short_volume],
                        keys_to_keep={'date', 'short_volume', 'total_volume', 'short_volume_ratio', 'exempt_volume',
                                      'non_exempt_volume'},
                        keys_to_add={'share_id': ticker_id_map[short_volume['ticker']]},
                        rename={'total_volume': 'f_volume'}
                    )
                    sv_list.extend(sv_list_item)
                # Update database
                upsert_data_smart(sv_list,'market_metrics', {'date', 'share_id'})

        # Short interest
        for i in range(0, len(tickers), 500):
            short_interests = polygon_service.request_short_interest(tickers=tickers[i:i+500],date=utils.get_utc_date(config.days))
            if short_interests:
                si_list = []
                for short_interest in short_interests:
                    si_list_item = utils.safe_filter_dict_keys(
                        data=[short_interest],
                        keys_to_keep= {'settlement_date', 'short_interest', 'avg_daily_volume', 'days_to_cover'},
                        keys_to_add= {'share_id': ticker_id_map[short_interest['ticker']]},
                        rename= {'settlement_date': 'date', 'avg_daily_volume': 'avg_f_volume', 'days_to_cover': 'short_interest_ratio'}
                    )
                    si_list.extend(si_list_item)
                # Update database
                upsert_data_smart(si_list,'market_metrics', {'date', 'share_id'})

    except Exception as e:
        logger.error(f"fetch_and_update_market_metrics couldn't update market metrics for date {utils.get_utc_date(config.days)}.")
import json
def run_aggregates_stream():
    handle_agg_insert_list = []
    us_market_open = utils.us_market_open_utc(get_utc_date(days=0,as_str=False))
    us_market_open_time = (utils.us_market_open_utc(get_utc_date(days=0,as_str=False))).time()
    us_market_close_time = (us_market_open + timedelta(hours=6.5)).time()
    existing_tickers = db_ops.get_existing_tickers()

    def handle_aggregates(msgs: List[WebSocketMessage]):
        # for m in msgs if isinstance(msgs, str) else []: todo this is for raw input
        for m in msgs:
            share_id =  existing_tickers.get(m.symbol)
            if share_id is None: continue
            bar_datetime = datetime.fromtimestamp(m.end_timestamp/1000, timezone.utc) # this here returns datetime that's 1 hour ahead of UTC. It must be UTC, not sure if it has something to do with my local time as matches it.
            insert_dict = {
                'datetime': bar_datetime,
                'abs_atr': None,
                'avg_volume': None,
                'close': int(m.close * 10_000),
                'convergence2': None,
                'convergence3': None,
                'high': int(m.high * 10_000),
                'low': int(m.low * 10_000),
                'open': int(m.open * 10_000),
                'rel_volume': None,
                'session': 0 if bar_datetime.time() < us_market_open_time else 1 if bar_datetime.time() < us_market_close_time else 2,
                'sma10': None,
                'volume': int(m.volume * 10_000),
                'vwap': int(m.vwap * 10_000),
                'share_id': share_id
            }
            handle_agg_insert_list.append(insert_dict)
        # Insert into database
        if handle_agg_insert_list and len(handle_agg_insert_list) > 2000:
            start_time = datetime.now(timezone.utc)
            upsert_data_smart(handle_agg_insert_list,'timeframe_1m',{'share_id','datetime'})
            elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
            print(f"Inserted in {elapsed:.2f} seconds")
            print(f"Inserted {len(handle_agg_insert_list)} rows")
            handle_agg_insert_list.clear()
    # Run steam
    ws = polygon_service.create_ws_client(["AM.*"], Feed.Delayed, Market.Stocks, False)
    ws.run(handle_msg=handle_aggregates)







