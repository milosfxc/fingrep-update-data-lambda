import logging
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import DictCursor

import config
from ConnType import DBLocation
from config import DB_NAME, DB_USER, LOCAL_DB_HOST, DB_PORT, DB_PASSWORD
import pandas as pd
logger = logging.getLogger(__name__)


@contextmanager
def get_db_connection():
    if config.db_location == DBLocation.REMOTE:
        conn = postgresql_remote_connection()
    else:
        conn = postgres_local_connection()

    try:
        yield conn  # Yield the connection to the caller
    finally:
        conn.close()  # Close the connection when done


def postgres_local_connection():
    conn = psycopg2.connect(database=DB_NAME, user=DB_USER, host=LOCAL_DB_HOST, port=DB_PORT, password=DB_PASSWORD)
    return conn


def postgresql_remote_connection():
    try:
        # Connect to the database through the tunnel
        connection = psycopg2.connect(
            host='127.0.0.1',  # Localhost for the tunnel
            port=config.LOCAL_BIND_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return connection

    except psycopg2.Error as db_error:
        logger.error(f"Remote db connection error: {db_error}")


def get_foreign_keys():
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                ans = {'indices': {}, 'currencies': {}}
                # Currencies
                cur.execute('SELECT symbol, id FROM currencies;')
                for record in cur:
                    ans['currencies'][record['symbol']] = record['id']
                # Indices
                cur.execute('SELECT ticker, id FROM indices;')
                for record in cur:
                    ans['indices'][record['ticker']] = record['id']
                return ans
    except (Exception, psycopg2.DatabaseError) as error:
        logger.exception(f"get_foreign_keys: {error}")
        raise


def upsert_dataframe_composite_id(df: pd.DataFrame, table_name: str):
    # Replace NaN with None
    df = df.astype(object).where(pd.notnull(df), None)
    # magnify columns
    columns_to_multiply = ['open', 'high', 'low', 'close', 'volume']
    df[columns_to_multiply] = (df[columns_to_multiply] * 10000).astype(int)
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
        with get_db_connection() as conn:
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
        logger.error(f"Error during database operation: {e}")


def upsert_dataframe(df: pd.DataFrame, table_name: str):
    # Replace NaN with None
    df = df.astype(object).where(pd.notnull(df), None)
    # Create a list of column update expressions for ON CONFLICT
    update_columns = ', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns if col not in ['id']])
    # Create the SQL query for upserting
    upsert_financials_query = f"""
        INSERT INTO {table_name} ({', '.join(df.columns)}) 
        VALUES ({', '.join(['%s'] * len(df.columns))})
        ON CONFLICT (ticker) DO UPDATE SET
        {update_columns};
    """
    try:
        # Open a connection to the PostgresSQL database
        with get_db_connection() as conn:
            # Open a cursor to perform database operations
            with conn.cursor() as cursor:
                # Iterate over each row in the DataFrame
                for row in df.itertuples(index=False, name=None):
                    cursor.execute(upsert_financials_query, row)
            # Commit the transaction
            conn.commit()
    except Exception as e:
        logger.error(f"Error during database operation: {e}")
