import logging
import psycopg2
from psycopg2.extras import DictCursor, execute_values
import os
import pandas as pd
logger = logging.getLogger(__name__)


def postgres_connection():
    host = os.getenv("FINGREP_POSTGRES_HOST")
    database = os.getenv("FINGREP_POSTGRES_DATABASE")
    user = os.getenv("FINGREP_POSTGRES_USER")
    password = os.getenv("FINGREP_POSTGRES_PASS")
    port = 5432
    conn = psycopg2.connect(database=database, user=user, host=host, port=port, password=password)
    return conn


def get_foreign_keys():
    conn = postgres_connection()
    try:
        cur = conn.cursor(cursor_factory=DictCursor)
        ans = {'indices': {}, 'currencies': {}}

        # Currencies
        cur.execute('SELECT symbol, id FROM currencies;')
        for record in cur:
            ans['currencies'][record['symbol']] = record['id']
        # Indices
        cur.execute('SELECT ticker, id FROM indices;')
        for record in cur:
            ans['indices'][record['ticker']] = record['id']

        cur.close()
        return ans
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"#get_foreign_keys: {error}")
        raise
    finally:
        if conn is not None:
            conn.close()


def upsert_dataframe_composite_id(df: pd.DataFrame, table_name: str):
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