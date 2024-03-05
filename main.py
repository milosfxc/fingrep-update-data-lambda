from db_ops import *
from datetime import datetime, timezone, timedelta
import os
import requests
import pandas as pd
from ta_utils import rsi_tv_new_tickers, rsi_tv_existing_tickers, atr_new_tickers, atr_existing_tickers, convergence
from bs4 import BeautifulSoup

# Pandas configuration
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 400)


def get_grouped_daily_bars():
    current_utc_date = datetime.utcnow() - timedelta(days=1)
    current_utc_date = current_utc_date.strftime("%Y-%m-%d")
    url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{current_utc_date}"

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
        raise Exception(f"Error: {response.status_code} - {response.text}")


def get_and_insert_aggregated_bars(ticker, ticker_id, date_from, limit):
    current_utc_date = datetime.utcnow().strftime("%Y-%m-%d")
    url = (f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{date_from}/{current_utc_date}"
           f"?adjusted=true&sort=asc&limit={limit}")
    params = {
        "apiKey": os.getenv("POLYGON_API_KEY")
    }

    response = requests.get(url, params=params)

    if response.status_code == 200:
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
        df_aggregated_daily['abs_atr'] = atr_new_tickers(df_aggregated_daily.copy())
        df_aggregated_daily['rel_atr'] = (df_aggregated_daily['abs_atr'] / df_aggregated_daily['close'] * 100).round(2)
        # df_aggregated_daily['avg_volume'] = df_aggregated_daily['volume'].rolling(window=20).mean()
        # df_aggregated_daily['sma10'] = df_aggregated_daily['close'].rolling(window=10).mean().round(4)
        # df_aggregated_daily['sma20'] = df_aggregated_daily['close'].rolling(window=20).mean().round(4)
        # df_aggregated_daily['sma50'] = df_aggregated_daily['close'].rolling(window=50).mean().round(4)
        # df_aggregated_daily['sma100'] = df_aggregated_daily['close'].rolling(window=100).mean().round(4)
        # df_aggregated_daily['sma200'] = df_aggregated_daily['close'].rolling(window=200).mean().round(4)
        # df_aggregated_daily['adr(%)'] = (100 * ((df_aggregated_daily['high'] / df_aggregated_daily['low'])
        #                                       .rolling(window=20).mean() - 1)).round(2)
        # df_aggregated_daily['adr($)'] = ((df_aggregated_daily['high'] - df_aggregated_daily['low'])
        #                                 .rolling(window=20).mean()).round(2)
        # df_aggregated_daily['rel_volume'] = (df_aggregated_daily['volume']/df_aggregated_daily['avg_volume']).round(2)
        # df_aggregated_daily['rsi'] = utils.rsi_tradingview(df_aggregated_daily)
        # df_aggregated_daily['atr'] = utils.calculate_atr(df_aggregated_daily)
        # print(df_aggregated_daily['atr'])
        try:
            df_aggregated_daily.to_sql('d_timeframe', con=postgres_engine(), if_exists='append', index=False,
                                       index_label=['share_id', 'date'])
        except Exception as e:
            print(f"#get_and_insert_aggregated_bars#{ticker}#Database insertion error: {e}")
            raise
    else:
        print(f"#get_and_insert_aggregated_bars: {response.status_code} - {response.text}")


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

    utc_now = datetime.utcnow().replace(tzinfo=timezone.utc).date()
    all_rows_current_utc_date = all(df['date'] == utc_now)
    if not all_rows_current_utc_date:
        all_tickers_count = len(df)
        # Change operator from != to == for premium Polygon.io plan
        df.query("date == @utc_now", inplace=True)
        current_date_tickers_count = len(df)
        print(
            f"Grouped daily bars API had {current_date_tickers_count} out of {all_tickers_count} tickers on {utc_now}")
    return df


def get_ticker_details_v3(ticker):
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"

    params = {
        "apiKey": os.getenv("POLYGON_API_KEY")
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json().get('results')
        if data:
            return data
        else:
            print(f"#get_ticker_details_v3({ticker}): Status code 200, but the results key was not present or empty.")
            return None
    else:
        print(f"#get_ticker_details_v3({ticker}): Error: {response.status_code} - {response.text}")


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


def get_new_ticker_data_and_insert(ticker):
    ticker_data = get_ticker_details_v3(ticker)
    finviz_data = get_finviz_sic(ticker)
    shares_data, shares_info_data = extract_ticker_details_v3(ticker_data, finviz_data, foreign_keys_db)
    ticker_id = insert_new_ticker(postgres_connection(), shares_data, shares_info_data)
    # Starter plan required for 2+ years historical data
    # three_years_before_now = datetime.utcnow().replace(tzinfo=timezone.utc).date() - timedelta(days=365 * 3)
    # Under basic plan up to 2 years historical data
    two_years_before_now = datetime.utcnow().replace(tzinfo=timezone.utc).date() - timedelta(days=365 * 2)
    ipo_date = datetime.strptime(shares_info_data['ipo_date'], '%Y-%m-%d').date()

    if ipo_date > two_years_before_now:
        get_and_insert_aggregated_bars(ticker, ticker_id, ipo_date, 5000)
    else:
        get_and_insert_aggregated_bars(ticker, ticker_id, two_years_before_now, 5000)


def update_atr_and_rsi_existing_tickers():
    df_last_100 = get_last_100()
    df_last_100.sort_values(by='date', ascending=True, inplace=True)
    df_last_100['rsi'] = df_last_100.groupby('share_id', as_index=False).apply(
        lambda group: rsi_tv_existing_tickers(group), include_groups=False).reset_index(level=0, drop=True)
    df_last_100['abs_atr'] = df_last_100.groupby('share_id', as_index=False).apply(
        lambda group: atr_existing_tickers(group), include_groups=False).reset_index(level=0, drop=True)
    df_last_100['rel_atr'] = (df_last_100['abs_atr'] / df_last_100['close'] * 100).round(2)
    convergence_tickers = df_last_100.groupby('share_id').apply(convergence, include_groups=False)
    df_last_100 = pd.merge(df_last_100, convergence_tickers.rename('convergence'), on=['share_id'], how='left')
    utc_now = datetime.utcnow().replace(tzinfo=timezone.utc).date() - timedelta(days=1)
    df_last_100 = df_last_100.query("date == @utc_now")
    df_last_100 = df_last_100.drop(columns=['close', 'high', 'low'])
    update_data = [(row['rsi'], row['abs_atr'], row['rel_atr'], row['share_id'], row['date'], row['convergence'])
                   for index, row in df_last_100.iterrows()]
    # Construct the SQL query
    sql_update = """UPDATE d_timeframe AS d
             SET rsi = t.rsi, abs_adr = t.abs_adr, rel_adr = t.rel_adr, convergence = t.convergence
             FROM (VALUES %s) AS t(rsi, abs_adr, rel_adr, share_id, date, convergence)
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
        return None
    except requests.exceptions.RequestException as e:
        print(f"#get_finviz_sic({ticker}): {e}")
        return None


# Get existing tickers and new daily data
existing_tickers = get_existing_tickers()
df_grouped_daily = get_grouped_daily_bars()

# Data frame for existing tickers
df_grouped_daily['id'] = df_grouped_daily['T'].map(existing_tickers)
df_grouped_daily_existing = df_grouped_daily.dropna(subset=['id'])

# Importing data for existing tickers
df_grouped_daily_existing = prepare_for_insert(df_grouped_daily_existing.copy())
try:
     df_grouped_daily_existing.to_sql('d_timeframe', con=postgres_engine(), if_exists='append', index=False,
                                      index_label=['share_id', 'date'])
except Exception as e:
    print(f"Error occurred while trying to insert daily data for existing tickers: {e}")

# Data frame for new tickers
df_grouped_daily_new = df_grouped_daily[df_grouped_daily['id'].isna()]
tickers_list = df_grouped_daily_new['T'].values.tolist()
counter = 0
foreign_keys_db = get_foreign_keys()
for new_ticker in tickers_list:
    get_new_ticker_data_and_insert(new_ticker)
    counter = counter + 1
    if counter == 2:
        break

# Update ATR and RSI for existing tickers
update_atr_and_rsi_existing_tickers()
