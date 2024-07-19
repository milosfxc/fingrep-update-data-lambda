import logging

from db_ops import *
from datetime import datetime, date, timezone, timedelta
import os
import requests
import pandas as pd

from ta_utils import rsi_tv_new_tickers, rsi_tv_existing_tickers
from bs4 import BeautifulSoup
from retry import retry
import constant
from fingrep_service import get_and_insert_fundamentals, get_and_insert_trading_info
logging.getLogger('root').setLevel(logging.ERROR)
# Pandas configuration
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 400)


def get_formatted_utc_date():
    current_utc_date = datetime.utcnow() - timedelta(days=constant.DAYS)
    return current_utc_date.strftime("%Y-%m-%d")


@retry(exceptions=requests.RequestException, tries=3, delay=2, backoff=2)
def get_grouped_daily_bars():
    url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{get_formatted_utc_date()}"

    params = {
        "adjusted": "true",
        "include_otc": "false",
        "apiKey": os.getenv("POLYGON_API_KEY")
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        df_grouped_daily_polygon = pd.DataFrame(data['results'])
        # Checks resultsCount and the actual results array length
        check_row_number(data['resultsCount'], len(df_grouped_daily_polygon))
        # Removes rows that contain at least one NaN OHLC value
        df_grouped_daily_polygon = check_nan_ohlc(df_grouped_daily_polygon)
        # Removes rows if the ticker column contains anything other than capital letters:
        df_grouped_daily_polygon = df_grouped_daily_polygon[df_grouped_daily_polygon['T'].str.isupper()]
        df_grouped_daily_polygon = df_grouped_daily_polygon[~df_grouped_daily_polygon['T'].str.contains('\.')]
        # Check if all tickers have the same UTC date
        check_one_date(df_grouped_daily_polygon)
        # Volume column conversion to integer
        df_grouped_daily_polygon['v'] = df_grouped_daily_polygon['v'].astype(int)
        return df_grouped_daily_polygon
    else:
        raise requests.RequestException(f"#get_grouped_daily_bars: {response.status_code} - {response.text}")


@retry(exceptions=requests.RequestException, tries=3, delay=2, backoff=2)
def get_and_insert_aggregated_bars(ticker, ticker_id, date_from, limit):
    url = (f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{date_from}/{get_formatted_utc_date()}"
           f"?adjusted=true&sort=asc&limit={limit}")
    params = {
        "apiKey": os.getenv("POLYGON_API_KEY")
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()
    df_aggregated_daily = pd.DataFrame(data['results'])
    # Volume column conversion to integer
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

    try:
        df_aggregated_daily.to_sql('d_timeframe', con=postgres_engine(), if_exists='append', index=False,
                                   index_label=['share_id', 'date'])
    except Exception as e:
        print(f"#get_and_insert_aggregated_bars#{ticker}#Database insertion error: {e}")
        raise


def check_nan_ohlc(df):
    tickers_with_nan = df.loc[df[['o', 'h', 'l', 'c']].isna().any(axis=1), 'T'].tolist()
    if tickers_with_nan:
        print("The following tickers had at least one NaN OHLC value on the date ", datetime.utcnow(), ":",
              tickers_with_nan)
        return df.dropna(subset=['o', 'h', 'l', 'c'])
    else:
        return df


def check_one_date(df):
    if not df['t'].nunique() == 1:
        print("#check_one_date#Not all tickers have the same date.")


def check_row_number(results_count, results_length):
    if results_count < 8000:
        print("resultsCount number is bellow 8000: ", results_count)
        print("Number of results: ", results_length)
    if results_count != results_length:
        print(f"resultCount length {results_count} doesn't match with the results length {results_length}")


def prepare_for_insert(df):
    df['date'] = pd.to_datetime(df['t'], unit='ms').dt.date
    df = df.drop(['T', 'n', 't'], axis=1)
    df['id'] = df['id'].astype(int)
    df.rename(
        columns={'v': 'volume', 'o': 'open', 'c': 'close', 'h': 'high', 'l': 'low', 'id': 'share_id', 'vw': 'vwap'},
        inplace=True)
    utc_now = datetime.utcnow().replace(tzinfo=timezone.utc).date() - timedelta(days=constant.DAYS)
    all_rows_current_utc_date = all(df['date'] == utc_now)
    if not all_rows_current_utc_date:
        all_tickers_count = len(df)
        # Change operator from != to == for premium Polygon.io plan
        df.query("date == @utc_now", inplace=True)
        current_date_tickers_count = len(df)
        print(
            f"Grouped daily bars API had {current_date_tickers_count} out of {all_tickers_count} tickers on {utc_now}")
    return df


@retry(exceptions=requests.RequestException, tries=3, delay=2, backoff=2)
def get_ticker_details_v3(ticker):
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"

    params = {
        "apiKey": os.getenv("POLYGON_API_KEY")
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json().get('results')
    return data


def extract_ticker_details_v3(ticker_details, finviz_data, foreign_keys):
    ticker_data = {
        'ticker': ticker_details.get('ticker'),
        'cik': ticker_details.get('cik'),
        'name': ticker_details.get('name'),
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


def insert_new_ticker(connection, shares, shares_info):
    try:
        # Begin the transaction
        with connection, connection.cursor() as cursor:
            # Insert into 'shares'
            insert_share_query = """
                INSERT INTO shares ({columns}) VALUES ({placeholders}) RETURNING id;
            """.format(columns=', '.join(shares.keys()), placeholders=', '.join(['%s'] * len(shares)))

            cursor.execute(insert_share_query, list(shares.values()))
            last_inserted_id = cursor.fetchone()[0]

            # Update 'shares_info' with the last_inserted_id
            shares_info['share_id'] = last_inserted_id

            # Insert into 'shares_info'
            insert_shares_info_query = """
                INSERT INTO shares_info ({columns}) VALUES ({placeholders});
            """.format(columns=', '.join(shares_info.keys()), placeholders=', '.join(['%s'] * len(shares_info)))

            cursor.execute(insert_shares_info_query, list(shares_info.values()))
            return shares_info['share_id']
    except psycopg2.Error or Exception as insert_error:
        # An exception will automatically trigger a rollback
        print(f"#insert_new_ticker_exception_block_1({shares.get('ticker')}): {insert_error}", )
    # No need to explicitly commit; it's handled by the 'with' block
    except Exception as e:
        print(f"#insert_new_ticker_exception_block_2({shares.get('ticker')}): {e}")


def get_new_ticker_data_and_insert(ticker, finviz_df):
    try:
        ticker_data = get_ticker_details_v3(ticker)
    except requests.exceptions.HTTPError as e:
        print(f"get_new_ticker_data_and_insert({ticker}): Couldn't obtain ticker details. "
              f"The HTTP status code: {e.response.status_code}")
        return

    finviz_ticker_df = finviz_df.query("ticker == @ticker")
    if not finviz_ticker_df.empty:
        finviz_data = {
            'sector': finviz_ticker_df['sector'].values[0],
            'industry': finviz_ticker_df['industry'].values[0],
            'country': finviz_ticker_df['country'].values[0]
        }
    else:
        finviz_data = get_finviz_sic(ticker)

    shares_data, shares_info_data = extract_ticker_details_v3(ticker_data, finviz_data, foreign_keys_db)
    ticker_id = insert_new_ticker(postgres_connection(), shares_data, shares_info_data)

    # Starter plan required for 2+ years historical data
    date_from = datetime.utcnow().replace(tzinfo=timezone.utc).date() - timedelta(days=365 * 2)
    get_and_insert_aggregated_bars(ticker, ticker_id, date_from, 5000)
    # Fundamental data and trade info
    cik = shares_info_data.get('cik')
    if cik is not None and ticker_id is not None:
        get_and_insert_trading_info(cik=cik, share_id=ticker_id, date=date(2019, 12, 30))
        get_and_insert_fundamentals(cik=cik, share_id=ticker_id, currency_id=shares_data.get('currency_id'),
                                    ticker=ticker, period='annual')


def update_rsi_existing_tickers():
    df_last_100 = get_last_100()
    df_last_100.sort_values(by='date', ascending=True, inplace=True)
    df_last_100['rsi'] = df_last_100.groupby('share_id', as_index=False).apply(
        lambda group: rsi_tv_existing_tickers(group), include_groups=False).reset_index(level=0, drop=True)
    utc_now = datetime.utcnow().replace(tzinfo=timezone.utc).date() - timedelta(days=constant.DAYS)
    df_last_100 = df_last_100.query("date == @utc_now")
    df_last_100 = df_last_100.drop(columns=['close', 'high', 'low'])
    df_last_100.dropna(subset=['rsi'], inplace=True)
    update_data = [(row['rsi'], row['share_id'], row['date'])
                   for index, row in df_last_100.iterrows()]
    # Construct the SQL query
    sql_update = """UPDATE d_timeframe AS d
             SET rsi = t.rsi
             FROM (VALUES %s) AS t(rsi, share_id, date)
             WHERE d.share_id = t.share_id AND d.date = t.date"""
    conn = postgres_connection()
    try:
        # Execute the query
        cur = conn.cursor()
        execute_values(cur, sql_update, update_data, page_size=len(update_data))
        conn.commit()
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"#update_atr_and_rsi_existing_tickers: {error}")
    finally:
        if conn is not None:
            conn.close()
    # Close the connection
    conn.close()


def get_finviz_sic(ticker):
    finviz_url = f"https://finviz.com/quote.ashx?t={ticker}&ty=c&p=d&b=1"
    headers = {'User-Agent': 'Mozilla/5.0'}

    try:
        page = requests.get(url=finviz_url, headers=headers, timeout=10)  # Timeout set to 10 seconds
        page.raise_for_status()  # Raise an HTTPError for bad requests
        soup = BeautifulSoup(page.content, 'html.parser')

        quote_links_div = soup.find('div', class_='quote-links')
        links_text = [link.text.strip() for link in quote_links_div.find_all('a')][:3]

        sic_data = {
            'sector': links_text[0],
            'industry': links_text[1],
            'country': links_text[2]
        }
        return sic_data
    except requests.exceptions.Timeout:
        print(f"#get_finviz_sic({ticker}):Request timed out.")
        return dict()
    except requests.exceptions.RequestException as e:
        print(f"#get_finviz_sic({ticker}): {e}")
        return dict()


@retry(exceptions=requests.RequestException, tries=3, delay=2, backoff=2)
def get_stock_splits():
    url = (f"https://api.polygon.io/v3/reference/splits?execution_date={get_formatted_utc_date()}"
           f"&reverse_split=true&limit=100")

    params = {
        "apiKey": os.getenv("POLYGON_API_KEY")
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json().get('results')
    return [entry['ticker'] for entry in data]


# Get existing tickers and new daily data
existing_tickers = get_existing_tickers()
df_grouped_daily = get_grouped_daily_bars()
# Data frame for existing tickers
df_grouped_daily['id'] = df_grouped_daily['T'].map(existing_tickers)
existing_tickers_id = [int(id) for id in df_grouped_daily['id'].dropna().tolist()]
df_grouped_daily_existing = df_grouped_daily.dropna(subset=['id'])
# Importing data for existing tickers
df_grouped_daily_existing = prepare_for_insert(df_grouped_daily_existing.copy())
# try:
#     df_grouped_daily_existing.to_sql('d_timeframe', con=postgres_engine(), if_exists='append', index=False,
#                                      index_label=['share_id', 'date'])
# except Exception as e:
#     print(f"Error occurred while trying to insert daily data for existing tickers: {e}")

# Data frame for new tickers
df_grouped_daily_new = df_grouped_daily[df_grouped_daily['id'].isna()]
tickers_list = df_grouped_daily_new['T'].values.tolist()
counter = 0
foreign_keys_db = get_foreign_keys()
finviz_df = pd.read_csv('data/finviz_sic.csv')
for new_ticker in tickers_list:
    get_new_ticker_data_and_insert(new_ticker, finviz_df)
    if counter == 1:
        break
    counter += 1

# Update ATR and RSI for existing tickers
#update_rsi_existing_tickers()


# Stock splits check
# tickers_split = get_stock_splits()
# for ticker in tickers_split:
#     if ticker in existing_tickers:
#         ticker_id = existing_tickers[ticker]
#         if delete_aggregate_bars(ticker_id):
#             date_from = datetime.utcnow().replace(tzinfo=timezone.utc).date() - timedelta(days=365 * 5)
#             get_and_insert_aggregated_bars(ticker, ticker_id, date_from, 5000)
#         else:
#             print(f"Couldn't delete and reinsert ticker {ticker} for stock split.")
