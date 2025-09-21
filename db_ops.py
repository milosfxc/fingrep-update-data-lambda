import datetime
import json
import logging
import traceback
from typing import Optional, Union, List, Dict

from edgar.formatting import accession_number_text
from psycopg2 import sql
from psycopg2.extras import DictCursor, execute_values, execute_batch
import pandas as pd

import utils
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
                       'currencies': {}, 'ticker_and_share_id_by_cik': {}}
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
                # Ticker and share_id by CIK
                cur.execute('SELECT s.id, s.ticker, i.cik FROM shares s JOIN shares_info i ON s.id = i.share_id WHERE i.cik > 0')
                for record in cur:
                    ans['ticker_and_share_id_by_cik'][record['cik']] = {'share_id': record['id'], 'ticker': record['ticker']}
                foreign_keys_cache = ans
                return foreign_keys_cache
    except psycopg2.DatabaseError as error:
        logger.error(f"get_foreign_keys: {error}")
        raise


def get_cached_foreign_keys() -> Optional[dict]:
    return foreign_keys_cache if foreign_keys_cache else get_foreign_keys()


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


def upsert_statement_v2(stmt_dict: dict, table_name: str, conflict_columns: list) -> bool:
    """
    Insert or update a row in PostgreSQL table (UPSERT).

    Args:
        stmt_dict: Dictionary where keys are column names and values are data
        table_name: Name of the table
        conflict_columns: List of columns that make up the composite key for conflict detection
    """
    try:
        # Convert dict values from NaN to None
        stmt_dict = utils.replace_dict_nan_with_none(stmt_dict)
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Prepare columns and values
                columns = ', '.join(stmt_dict.keys())
                placeholders = ', '.join(['%s'] * len(stmt_dict))

                # Prepare the UPDATE part (exclude conflict columns from being updated)
                update_set = ', '.join([
                    f"{col} = EXCLUDED.{col}"
                    for col in stmt_dict.keys()
                    if col not in conflict_columns
                ])

                # Build the full UPSERT query
                query = f"""
                    INSERT INTO {table_name} ({columns})
                    VALUES ({placeholders})
                    ON CONFLICT ({', '.join(conflict_columns)})
                    DO UPDATE SET {update_set}
                """

                # Print query for manual testing
                values = list(stmt_dict.values())
                debug_query = query
                for v in values:
                    val = f"'{v}'" if isinstance(v, str) else str(v)
                    debug_query = debug_query.replace('%s', val, 1)


                # Execute with parameterized values
                cur.execute(query, list(stmt_dict.values()))
                conn.commit()
                return True
    except psycopg2.DatabaseError as e:
        logger.error(f"insert_statement_v2 failed: {e}")
        logger.error(debug_query)
        logger.error(pd.DataFrame.from_dict(stmt_dict,"index"))
        return False

def query_3_quarter_sums(share_id: int, stmt_columns: set, table_name: str, start_date:datetime.date, end_date:datetime.date):

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Build the column sum expressions
                sum_expressions = [f"SUM({col}) AS {col}" for col in stmt_columns]

                query = f"""
                    WITH last_3 AS (
                        SELECT 
                            COUNT(*) AS row_count,
                            {', '.join(sum_expressions)}
                        FROM {table_name}
                        WHERE share_id = %s
                          AND date BETWEEN %s AND %s
                          AND report_type = 'q'
                    )
                    SELECT {', '.join(stmt_columns)}
                    FROM last_3 
                    WHERE row_count = 3
                """
                cur.execute(query, (share_id, start_date, end_date))
                result = cur.fetchone()

                if result is None:
                    return None

                # Convert to dictionary with float values
                return {
                    col: float(value) if value is not None else 0.0
                    for col, value in zip(stmt_columns, result)
                }

    except psycopg2.DatabaseError as e:
        logger.error(f"insert_statement_v2 failed: {e}")
        raise



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


def upsert_latest_filings(filings: list[dict]) -> bool:
    if not filings:
        return True

    query = """
        INSERT INTO latest_filings (
            accession_number,
            cik,
            share_id,
            filing_date,
            form,
            inserted,
            attempt_date
        ) VALUES (
            %(accession_number)s,
            %(cik)s,
            %(share_id)s,
            %(filing_date)s,
            %(form)s,
            %(inserted)s,
            %(attempt_date)s
        )
        ON CONFLICT (accession_number)
        DO UPDATE SET
            cik = EXCLUDED.cik,
            share_id = EXCLUDED.share_id,
            filing_date = EXCLUDED.filing_date,
            form = EXCLUDED.form,
            inserted = EXCLUDED.inserted,
            attempt_date = EXCLUDED.attempt_date;
    """

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                execute_batch(cur, query, filings, page_size=100)
            conn.commit()
        return True

    except psycopg2.DatabaseError as e:
        logger.error(f"upsert_latest_filings: {e}")
        if 'conn' in locals():
            conn.rollback()
        return False


def get_latest_filings_for_insert():
    sql_select = """SELECT sh.id AS share_id, sh.ticker, lf.cik, lf.accession_number, lf.filing_date, lf.form FROM latest_filings lf INNER JOIN shares_info si ON lf.cik = si.cik 
    INNER JOIN shares sh ON sh.id = si.share_id 
    WHERE inserted = FALSE 
    AND lf.filing_date < NOW() - INTERVAL '5 DAYS'
    AND ((lf.attempt_date BETWEEN NOW() - INTERVAL '30 DAYS' AND NOW() - INTERVAL '7 DAYS') OR lf.attempt_date IS NULL)
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
        return pd.DataFrame(data=None)


def delete_fillings_older_than_month():
    sql_delete = """DELETE FROM latest_filings WHERE filing_date < NOW() - INTERVAL '1 MONTH' AND inserted = TRUE"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_delete)
                conn.commit()
    except psycopg2.DatabaseError as e:
        logger.error(f"delete_old_fillings: {e}")



def get_accession_numbers() -> set[str] | None:
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                query = 'SELECT accession_number FROM latest_filings'
                cur.execute(query)
                if rows := cur.fetchall():
                    return {row[0] for row in rows}
                else:
                    return None
    except Exception as e:
        tb = traceback.extract_tb(e.__traceback__)
        file_path, line_no, func_name, text = tb[-1]
        logger.warning(f"{file_path}:{line_no} - Error occurred during data retrieval from latest_filings table: \n{e}")
        return None


def query_previous_cash_flow_statement(share_id: int, start_date:str, end_date:str) -> Optional[dict[str,Union[int,float,None]]]:

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT * FROM cash_flow_statement 
                    WHERE share_id = %s 
                    AND report_type = 'q' 
                    AND date > %s 
                    AND date < %s 
                    ORDER BY date DESC 
                    LIMIT 1;
                """

                cur.execute(query, (share_id, start_date, end_date))
                result = cur.fetchone()

                if result:
                    # Get column names from cursor description
                    columns = [desc[0] for desc in cur.description]
                    # Convert result tuple to dictionary with proper types
                    return {
                        col: val if val is not None else None
                        for col, val in zip(columns, result)
                    }
                return None

    except psycopg2.DatabaseError as e:
        logger.error(f"upsert_statement_v2 failed: {e}")
        raise


# EDGAR Database
@contextmanager
def get_edgar_db_connection():
    conn = psycopg2.connect(database='edgar', user=DB_USER, host=LOCAL_DB_HOST, port=DB_PORT, password=DB_PASSWORD)
    try:
        yield conn
    finally:
        conn.close()


def insert_filing(filing_id, filing: dict):
    """
    Function stores edgar and yahoo filings into filings database.
    :param filing_id: Ticker or Accession Number
    :param filing: Dict
    """
    try:
        # Convert dict NaN values to None
        filing = utils.replace_dict_nan_with_none(filing)
        with get_edgar_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO filings (filing_id, data, insert_date)
                    VALUES (%s, %s, CURRENT_DATE)
                    ON CONFLICT (filing_id) DO NOTHING;
                """,(filing_id, json.dumps(filing,default=str)))
            conn.commit()
    except Exception as e:
        tb = traceback.extract_tb(e.__traceback__)
        file_path, line_no, func_name, text = tb[-1]
        logger.error(f"{file_path}:{line_no} - Error occurred during insertion for ticker or accession number {filing_id}: \n{e}")



def get_filings_by_filing_id(filing_id: str, insert_date:str = None) -> dict | None:
    """
    Functions retrieves data from filings database.
    :param filing_id: Ticker or Accession Number
    :param insert_date: Optional argument to filter by insertion date
    :return: Dict or None
    """
    try:
        with get_edgar_db_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT data 
                    FROM filings 
                    WHERE filing_id = %s
                """
                params = [filing_id]

                if insert_date:
                    query += " AND DATE(insert_date) = %s"
                    params.append(insert_date)

                cur.execute(query, params)
                if result := cur.fetchone():
                    data = result[0]
                    if isinstance(data, str):
                        return json.loads(data)
                    else:
                        return data
                return None

    except Exception as e:
        tb = traceback.extract_tb(e.__traceback__)
        file_path, line_no, func_name, text = tb[-1]
        logger.warning(f"{file_path}:{line_no} - Error occurred during data retrieval for ticker or accession number {filing_id}: \n{e}")
        return None


