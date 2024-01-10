import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime
import os
import requests
import pandas as pd
from datetime import timedelta
from utils import get_finviz_sector_and_industry_and_country
from sqlalchemy import create_engine

# Pandas configuration
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 400)


def get_existing_tickers():
    conn = None
    try:
        conn = psycopg2.connect(
            host=os.getenv("FINGREP_POSTGRES_HOST"),
            database=os.getenv("FINGREP_POSTGRES_DATABASE"),
            user=os.getenv("FINGREP_POSTGRES_USER"),
            password=os.getenv("FINGREP_POSTGRES_PASS"))
        cur = conn.cursor(cursor_factory=DictCursor)

        # execute a statement
        cur.execute('SELECT ticker, id FROM shares;')
        ticker_dict = {}
        for record in cur:
            ticker_dict[record['ticker']] = record['id']

        cur.close()
        return ticker_dict
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def get_foreign_keys():
    conn = None
    try:
        conn = psycopg2.connect(
            host=os.getenv("FINGREP_POSTGRES_HOST"),
            database=os.getenv("FINGREP_POSTGRES_DATABASE"),
            user=os.getenv("FINGREP_POSTGRES_USER"),
            password=os.getenv("FINGREP_POSTGRES_PASS"))
        cur = conn.cursor(cursor_factory=DictCursor)

        # Countries
        cur.execute('SELECT country_name, id FROM countries;')
        ans = {'countries': {}, 'sectors': {}, 'industries': {}, 'share_types': {}, 'exchanges': {}, 'currencies': {}}
        for record in cur:
            ans['countries'][record['country_name']] = record['id']
        # Sectors
        cur.execute('SELECT sector_name, id FROM sectors;')
        for record in cur:
            ans['sectors'][record['sector_name']] = record['id']
        # Industries
        cur.execute('SELECT industry_name, id FROM industries;')
        for record in cur:
            ans['industries'][record['industry_name']] = record['id']
        # Share types
        cur.execute('SELECT short_name, id FROM share_types;')
        for record in cur:
            ans['share_types'][record['short_name']] = record['id']
        # Exchanges
        cur.execute('SELECT mic, id FROM exchanges;')
        for record in cur:
            ans['exchanges'][record['mic']] = record['id']
        # Currencies
        cur.execute('SELECT symbol, id FROM currencies;')
        for record in cur:
            ans['currencies'][record['symbol']] = record['id']

        cur.close()
        return ans
    except (Exception, psycopg2.DatabaseError) as error:
        print("#get_foreign_keys: Couldn't get foreign keys.")
        return None
    finally:
        if conn is not None:
            conn.close()


def get_grouped_daily_bars():
    current_utc_date = datetime.utcnow() - timedelta(days=3)
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
        df_grouped_daily = pd.DataFrame(data['results'])
        # Checks resultsCount and the actual results array length
        check_row_number(data['resultsCount'], len(df_grouped_daily))
        # Removes rows that contain at least one NaN OHLC value
        df_grouped_daily = check_nan_ohlc(df_grouped_daily)
        # Removes rows if the ticker column contains anything other than capital letters:
        df_grouped_daily = df_grouped_daily[df_grouped_daily['T'].str.isupper()]
        df_grouped_daily = df_grouped_daily[~df_grouped_daily['T'].str.contains('\.')]
        # Check if all tickers have the same UTC date
        check_one_date(df_grouped_daily)
        # Volume column conversion to integer
        df_grouped_daily['v'] = df_grouped_daily['v'].astype(int)
        return df_grouped_daily

    else:
        print(f"Error: {response.status_code} - {response.text}")


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
        print("Not all tickers have the same date.")


def check_row_number(resultsCount, resultsLength):
    if resultsCount < 8000:
        print("resultsCount number is bellow 8000: ", resultsCount)
        print("Number of results: ", resultsLength)
    if resultsCount != resultsLength:
        print(f"resultCount length {resultsCount} doesn't match with the results length {resultsLength}")


def prepare_for_insert(df):
    df['date'] = pd.to_datetime(df['t'], unit='ms').dt.date
    df = df.drop(['vw', 'T', 'n', 't'], axis=1)
    df['id'] = df['id'].astype(int)
    df.rename(columns={'v': 'volume', 'o': 'open', 'c': 'close', 'h': 'high', 'l': 'low', 'id': 'share_id'},
              inplace=True)
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


def extract_ticker_details_v3(ticker_details, foreign_keys, scraped_data):
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
        'sector_id': foreign_keys['sectors'].get(scraped_data.get('sector')),
        'industry_id': foreign_keys['industries'].get(scraped_data.get('industry')),
        'country_id': foreign_keys['countries'].get(scraped_data.get('country'))
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

    except psycopg2.Error or Exception as e:
        # An exception will automatically trigger a rollback
        print(f"#insert_new_ticker({shares.get('ticker')}): ", e)
    # No need to explicitly commit; it's handled by the 'with' block
    except Exception as e:
        print(f"#insert_new_ticker({shares.get('ticker')}): ", e)



#  Postgres params
host = os.getenv("FINGREP_POSTGRES_HOST")
database = os.getenv("FINGREP_POSTGRES_DATABASE")
user = os.getenv("FINGREP_POSTGRES_USER")
password = os.getenv("FINGREP_POSTGRES_PASS")
port = 5432
engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")
conn = psycopg2.connect(database=database, user=user, host=host, port=port, password=password)
# # Get existing tickers and new daily data
# df_grouped_daily = get_grouped_daily_bars()
# existing_tickers = get_existing_tickers()
#
# # Data frame for existing tickers
# df_grouped_daily['id'] = df_grouped_daily['T'].map(existing_tickers)
# df_grouped_daily_existing = df_grouped_daily.dropna(subset=['id'])


# Data frame for new tickers
# df_grouped_daily_new = df_grouped_daily[df_grouped_daily['id'].isna()]
# loop over tickers that aren't in the database
ticker_data = get_ticker_details_v3('CCJ')
finviz_data = get_finviz_sector_and_industry_and_country(
    'CCJ')  # maybe I should add here an if statement to try to scrape with yfinance
shares_data, shares_info_data = extract_ticker_details_v3(ticker_data, get_foreign_keys(), finviz_data)
insert_new_ticker(conn, shares_data, shares_info_data)

# Inserting new ticker


# Importing data for existing tickers
# df_grouped_daily_existing = prepare_for_insert(df_grouped_daily_existing.copy())
# df_grouped_daily_existing.to_sql('d_timeframe', con=engine, if_exists='append', index=False, index_label=['share_id', 'date'])
# get_ticker_details_v3('NUKK')
