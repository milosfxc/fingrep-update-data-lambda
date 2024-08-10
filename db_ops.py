import datetime
import logging

import psycopg2
from psycopg2.extras import DictCursor, execute_values
from sqlalchemy import create_engine, Table, MetaData
import os
import pandas as pd
logger = logging.getLogger(__name__)

from sqlalchemy.dialects.postgresql import insert


def postgres_connection():
    host = os.getenv("FINGREP_POSTGRES_HOST")
    database = os.getenv("FINGREP_POSTGRES_DATABASE")
    user = os.getenv("FINGREP_POSTGRES_USER")
    password = os.getenv("FINGREP_POSTGRES_PASS")
    port = 5432
    conn = psycopg2.connect(database=database, user=user, host=host, port=port, password=password)
    return conn


def postgres_engine():
    host = os.getenv("FINGREP_POSTGRES_HOST")
    database = os.getenv("FINGREP_POSTGRES_DATABASE")
    user = os.getenv("FINGREP_POSTGRES_USER")
    password = os.getenv("FINGREP_POSTGRES_PASS")
    port = 5432
    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")
    return engine


def postgres_engine_v2():
    host = os.getenv("FINGREP_POSTGRES_HOST")
    database = 'fingrep_v2'
    user = os.getenv("FINGREP_POSTGRES_USER")
    password = os.getenv("FINGREP_POSTGRES_PASS")
    port = 5432
    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")
    return engine


def get_existing_tickers():
    conn = postgres_connection()
    try:
        cur = conn.cursor(cursor_factory=DictCursor)

        # execute a statement
        cur.execute('SELECT ticker, id FROM shares;')
        ticker_dict = {}
        for record in cur:
            ticker_dict[record['ticker']] = record['id']

        cur.close()
        return ticker_dict
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"#get_existing_tickers: {error}")
        raise
    finally:
        if conn is not None:
            conn.close()


def get_last_100():
    try:
        query = """
        SELECT share_id, date, close, high, low
        FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY share_id ORDER BY date DESC) AS rn
        FROM d_timeframe
        ) AS subquery
        WHERE rn <= 100;
        """
        # execute a statement
        df = pd.read_sql_query(query, postgres_engine())
        return df

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"#get_last_100: {error}")
        raise


def get_all():
    try:
        query = """
        SELECT share_id, date, open, high, low, close, volume
        FROM d_timeframe;
        """
        # execute a statement
        df = pd.read_sql_query(query, postgres_engine_v2())
        return df

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"#get_last_100: {error}")
        raise


def get_foreign_keys():
    conn = postgres_connection()
    try:
        cur = conn.cursor(cursor_factory=DictCursor)

        # Countries
        cur.execute('SELECT name, id FROM countries;')
        ans = {'countries': {}, 'sectors': {}, 'industries': {}, 'share_types': {}, 'exchanges': {}, 'currencies': {}}
        for record in cur:
            ans['countries'][record['name']] = record['id']
        # Sectors
        cur.execute('SELECT name, id FROM sectors;')
        for record in cur:
            ans['sectors'][record['name']] = record['id']
        # Industries
        cur.execute('SELECT name, id FROM industries;')
        for record in cur:
            ans['industries'][record['name']] = record['id']
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
        print(f"#get_foreign_keys: {error}")
        raise
    finally:
        if conn is not None:
            conn.close()


def upsert_dataframe(df: pd.DataFrame, table_name: str):
    # Replace NaN with None
    df = df.astype(object).where(pd.notnull(df), None)
    # Create a list of column update expressions for ON CONFLICT
    update_columns = ', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns if col not in ['index_id', 'date']])
    # Create the SQL query for upserting
    upsert_financials_query = f"""
        INSERT INTO {table_name} ({', '.join(df.columns)}) 
        VALUES ({', '.join(['%s'] * len(df.columns))})
        ON CONFLICT (index_id, date) DO UPDATE SET
        {update_columns};
    """
    try:
        #logging.info("Starting database connection.")
        # Open a connection to the PostgreSQL database
        with postgres_connection() as conn:
            #logging.info("Database connection established.")
            # Open a cursor to perform database operations
            with conn.cursor() as cursor:
                #logging.info("Cursor created, starting data upsert.")
                # Iterate over each row in the DataFrame
                for row in df.itertuples(index=False, name=None):
                    cursor.execute(upsert_financials_query, row)
                #logging.info("Data upsert completed, committing the transaction.")
            # Commit the transaction
            conn.commit()
    except Exception as e:
        logging.error(f"Error during database operation: {e}")
        if 'conn' in locals():
            conn.rollback()
            logging.info("Transaction rolled back due to error.")



