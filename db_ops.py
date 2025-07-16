import json
import logging

from edgar.formatting import accession_number_text
from psycopg2.extras import DictCursor, execute_values
import pandas as pd

from config import DB_NAME, DB_USER, LOCAL_DB_HOST, DB_PORT, DB_PASSWORD
import config
from ConnType import DBLocation
from contextlib import contextmanager
import psycopg2

# Logger
logger = logging.getLogger(__name__)


# Connections

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


# CRUD functions for fingrep db

def get_existing_tickers():
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                # execute a statement
                cur.execute('SELECT ticker, id FROM shares;')
                ticker_dict = {}
                for record in cur:
                    ticker_dict[record['ticker']] = record['id']
                return ticker_dict
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"#get_existing_tickers: {error}")
        raise


def get_banned_tickers():
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                # execute a statement
                cur.execute("SELECT ticker, id FROM banned_tickers WHERE ban_date >= NOW() - INTERVAL '3 MONTHS';")
                ticker_dict = {}
                for record in cur:
                    ticker_dict[record['ticker']] = record['id']

                return ticker_dict
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"#get_banned_tickers: {error}")
        raise


def insert_banned_ticker(ticker: str):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # execute a parameterized statement
                cur.execute("INSERT INTO banned_tickers (ticker, ban_date) VALUES (%s, NOW());", (ticker,))
                conn.commit()  # Commit the transaction
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"#insert_banned_ticker: {error}")
        raise


def get_last_100():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                query = """
                SELECT share_id, date, close, high, low
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY share_id ORDER BY date DESC) AS rn
                    FROM d_timeframe
                ) AS subquery
                WHERE rn <= 100;
                """
                cur.execute(query)
                rows = cur.fetchall()
                # Get column names
                column_names = [desc[0] for desc in cur.description]
                # Convert the result set to a pandas DataFrame
                df = pd.DataFrame(rows, columns=column_names)

                return df
    except psycopg2.DatabaseError as error:
        logger.error(f"get_last_100: {error}")
        raise


foreign_keys_cache = None


def get_foreign_keys():
    global foreign_keys_cache
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                # Countries
                cur.execute('SELECT name, id FROM countries;')
                ans = {'countries': {}, 'sectors': {}, 'industries': {}, 'share_types': {}, 'exchanges': {},
                       'currencies': {}}
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
                foreign_keys_cache = ans
                return foreign_keys_cache
    except psycopg2.DatabaseError as error:
        logger.error(f"get_foreign_keys: {error}")
        raise


def delete_aggregate_bars(ticker_id: str):
    try:
        with get_db_connection() as conn:
            delete_statement = "DELETE FROM d_timeframe WHERE share_id = %s"
            with conn.cursor() as cur:
                cur.execute(delete_statement, (ticker_id,))
                conn.commit()
                return True
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"delete_aggregate_bars: {error}")
        return False


def upsert_dataframe(df: pd.DataFrame, table_name: str) -> bool:
    # Replace NaN with None and False with None
    df = df.astype(object).where(pd.notnull(df), None).replace(0, None)
    # Create a list of column update expressions for ON CONFLICT
    update_columns = ', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns if col not in ['share_id', 'date']])
    # Composite key is different for trade_info and fundamentals(bs,is,cf,ra)
    composite_key = "(share_id, date, report_type)" if table_name in {"balance_sheet", "income_statement",
                                                                      "cash_flow"} else "(share_id, date)"
    # Create the SQL query for upserting
    upsert_query = f"""
        INSERT INTO {table_name} ({', '.join(df.columns)}) 
        VALUES ({', '.join(['%s'] * len(df.columns))})
        ON CONFLICT {composite_key} DO UPDATE SET
        {update_columns};
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                row_count = 0  # Track affected rows
                for row in df.itertuples(index=False, name=None):
                    cur.execute(upsert_query, row)
                    row_count += cur.rowcount
                conn.commit()
                return row_count > 0
    except Exception as e:
        logging.error(f"upsert_dataframe: {e}")
        return False


def update_market_breadth(date: str):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:  # Note the parentheses here
                cur.execute("SELECT update_market_breadth(%s)", (date,))
                # Commit the transaction
                conn.commit()
    except psycopg2.DatabaseError as e:
        logger.error(f"update_market_breadth: {date} {e}")


def upsert_dataframe_v2(df: pd.DataFrame, table_name: str):
    # Replace NaN with None
    df = df.astype(object).where(pd.notnull(df), None)
    # Create a list of column update expressions for ON CONFLICT
    update_columns = ', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns if col not in ['share_id', 'date']])
    # Create the SQL query for upserting
    upsert_financials_query = f"""
        INSERT INTO {table_name} ({', '.join(df.columns)}) 
        VALUES ({', '.join(['%s'] * len(df.columns))})
        ON CONFLICT (share_id, date) DO UPDATE SET
        {update_columns};
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Use batch insert for efficiency
                cur.executemany(upsert_financials_query, df.itertuples(index=False, name=None))
                # Commit the transaction
                conn.commit()
    except psycopg2.DatabaseError as e:
        logger.error(f"upsert_dataframe_v2: {e}")


def insert_new_ticker(shares, shares_info):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Insert into 'shares'
                insert_share_query = """
                    INSERT INTO shares ({columns}) VALUES ({placeholders}) RETURNING id;
                """.format(columns=', '.join(shares.keys()), placeholders=', '.join(['%s'] * len(shares)))
                cur.execute(insert_share_query, list(shares.values()))
                last_inserted_id = cur.fetchone()[0]
                # Update 'shares_info' with the last_inserted_id
                shares_info['share_id'] = last_inserted_id
                # Insert into 'shares_info'
                insert_shares_info_query = """
                    INSERT INTO shares_info ({columns}) VALUES ({placeholders});
                """.format(columns=', '.join(shares_info.keys()), placeholders=', '.join(['%s'] * len(shares_info)))
                cur.execute(insert_shares_info_query, list(shares_info.values()))
                conn.commit()

                return shares_info['share_id']
    except psycopg2.Error as e:
        # Rollback will happen automatically if an error occurs
        logger.error(f"insert_new_ticker_exception_block_1 - for ticker {shares.get('ticker')}: {e}")


def update_rsi(data):
    sql_update = """UPDATE d_timeframe AS d
             SET rsi = t.rsi
             FROM (VALUES %s) AS t(rsi, share_id, date)
             WHERE d.share_id = t.share_id AND d.date = t.date"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, sql_update, data, page_size=len(data))
                conn.commit()
    except psycopg2.DatabaseError as error:
        logger.error(f"update_rsi: {error}")


def get_id_and_ticker_by_cik(cik: str):
    sql_select = """SELECT s.id, s.ticker FROM shares s
                     INNER JOIN shares_info i ON s.id = i.share_id 
                     WHERE i.cik = %s"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_select, (cik,))
                result = cur.fetchone()  # Fetch the first result
                if result:  # Check if a result was found
                    return {
                        'id': result[0],
                        'ticker': result[1],
                    }
                else:
                    return None
    except psycopg2.DatabaseError as e:
        logger.error(f"get_id_and_ticker_by_cik: {e}")
        return None


import pandas as pd


def get_ids_by_cik(cik_list: list) -> pd.DataFrame:
    sql_select = """SELECT i.share_id, i.cik FROM shares_info i
                    WHERE i.cik = ANY(%s::integer[])"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_select, (cik_list,))
                # Directly create DataFrame from cursor
                df = pd.DataFrame(cur.fetchall(), columns=['share_id', 'cik'])
                return df
    except psycopg2.DatabaseError as e:
        logger.error(f"get_ids_by_cik: {e}")
        # Return empty DataFrame with correct columns on error
        return pd.DataFrame(columns=['share_id', 'cik'])


# Latest fillings
def get_latest_filings_by_max_filing_date():
    sql_select = """SELECT * FROM latest_filings WHERE filing_date = (SELECT MAX(filing_date) FROM latest_filings)"""
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(sql_select)
                result = cur.fetchall()
                return [dict(row) for row in result] if result else None
    except psycopg2.DatabaseError as e:
        logger.error(f"get_latest_filings_by_max_filing_date: {e}")
        return None


def upsert_latest_filings(df: pd.DataFrame, upsert_fully_inserted_and_attempt_date: bool = False):
    # Replace NaN with None
    df = df.astype(object).where(pd.notnull(df), None)

    # Prepare the query
    if not upsert_fully_inserted_and_attempt_date:
        upsert_query = f"""
            INSERT INTO latest_filings ({', '.join(df.columns)}) 
            VALUES %s
            ON CONFLICT (accession_number, cik) DO UPDATE 
            SET partially_inserted = EXCLUDED.partially_inserted;
        """
    else:
        upsert_query = f"""
            INSERT INTO latest_filings ({', '.join(df.columns)}) 
            VALUES %s
            ON CONFLICT (accession_number, cik) DO UPDATE 
            SET fully_inserted = EXCLUDED.fully_inserted, 
            full_insert_attempt_date = EXCLUDED.full_insert_attempt_date;
        """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Convert to list of tuples
                data = list(df.itertuples(index=False, name=None))
                execute_values(cur, upsert_query, data)
                conn.commit()
                return True
    except psycopg2.DatabaseError as e:
        logger.error(f"upsert_latest_filings: {e}")
        return False


def get_filings_for_full_insert():
    sql_select = """SELECT sh.id, sh.ticker, lf.cik, lf.accession_number, lf.filing_date FROM latest_filings lf INNER JOIN shares_info si ON lf.cik = si.cik 
    INNER JOIN shares sh ON sh.id = si.share_id 
    WHERE fully_inserted = FALSE 
    AND lf.filing_date < NOW() - INTERVAL '4 DAYS'
    AND lf.filing_date < (DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 day')::DATE
    AND ((lf.full_insert_attempt_date BETWEEN NOW() - INTERVAL '30 DAYS' AND NOW() - INTERVAL '7 DAYS') OR lf.full_insert_attempt_date IS NULL)
    """

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_select)
                columns = [desc[0] for desc in cur.description]  # Get column names
                results = cur.fetchall()

                if results:
                    return pd.DataFrame(results, columns=columns)
                return pd.DataFrame()
    except psycopg2.DatabaseError as e:
        logger.error(f"get_filings_older_than_four_days: {e}")
        return None


def delete_fillings_older_than_month():
    sql_delete = """DELETE FROM latest_filings WHERE filing_date < NOW() - INTERVAL '1 MONTH' AND fully_inserted = TRUE"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_delete)
                conn.commit()
    except psycopg2.DatabaseError as e:
        logger.error(f"delete_old_fillings: {e}")


# Filings table
@contextmanager
def get_filings_db_connection():
    conn = psycopg2.connect(database='filings', user=DB_USER, host=LOCAL_DB_HOST, port=DB_PORT, password=DB_PASSWORD)
    try:
        yield conn
    finally:
        conn.close()


def insert_filing(accession_number, filing: dict):

    with get_filings_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO filings (accession_number, data)
                VALUES (%s, %s)
                ON CONFLICT (accession_number) DO NOTHING;
            """,(accession_number, json.dumps(filing)))
        conn.commit()

def get_filings_by_accession_numbers(accession_numbers: list[str]) -> dict[str,dict]:
    with get_filings_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT accession_number, data 
                FROM filings 
                WHERE accession_number = ANY(%s);
            """, (accession_numbers,))
            return {row[0]: row[1] for row in cur.fetchall()}