import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime
import os
import requests
import pandas as pd
from datetime import timedelta
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
    df.rename(columns={'v': 'volume', 'o': 'open', 'c': 'close', 'h': 'high', 'l': 'low', 'id': 'share_id'}, inplace=True)
    return df

def get_ticker_details_v3(ticker):
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"

    params = {
        "apiKey": os.getenv("POLYGON_API_KEY")
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        print(data)
        return data

    else:
        print(f"Error: {response.status_code} - {response.text}")
# # Get existing tickers and new daily data
# df_grouped_daily = get_grouped_daily_bars()
# existing_tickers = get_existing_tickers()
#
# # Data frame for existing tickers
# df_grouped_daily['id'] = df_grouped_daily['T'].map(existing_tickers)
# df_grouped_daily_existing = df_grouped_daily.dropna(subset=['id'])
# # Data frame for new tickers
# df_grouped_daily_new = df_grouped_daily[df_grouped_daily['id'].isna()]
#
#
# df_grouped_daily_existing = prepare_for_insert(df_grouped_daily_existing.copy())
# host = os.getenv("FINGREP_POSTGRES_HOST")
# database = os.getenv("FINGREP_POSTGRES_DATABASE")
# user = os.getenv("FINGREP_POSTGRES_USER")
# password = os.getenv("FINGREP_POSTGRES_PASS")
# port = 5432
# engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")
# df_grouped_daily_existing.to_sql('d_timeframe', con=engine, if_exists='append', index=False, index_label=['share_id', 'date'])
#get_ticker_details_v3('NUKK')
