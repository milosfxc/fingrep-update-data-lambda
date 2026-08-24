import pandas as pd
import config
from config import logger
from clickhouse_driver import Client
from datetime import date, timedelta
from config import DB_PASSWORD, DB_NAME


def clickhouse_local_connection():
    return Client(
        host='localhost',
        port=9000,
        user='fingrep_writer',
        password=DB_PASSWORD,
        database= DB_NAME
    )


def upsert_grouped_daily(df: pd.DataFrame, table_name: str):
    if df.empty:
        return
    # Replace NaN with None
    df = df.where(pd.notnull(df), None)
    try:
        with clickhouse_local_connection() as conn:
            filter_keys = ', '.join(f"({share_id}, '{date_obj}')"
                for share_id, date_obj in zip(df['share_id'], df['date']))
            # Check if any rows already exist
            check_query = f"""
                SELECT 1
                FROM {table_name}
                WHERE (share_id, date) IN ({filter_keys})
                LIMIT 1
            """
            # Delete only if duplicates exist
            is_duplicate = bool(conn.execute(check_query))
            if is_duplicate:
                repair_d_aggregates(df=df, timeframes=['D', 'W', 'M', 'Q'])
            # Insert new rows
            insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES"
            conn.execute(insert_query, df.to_dict('records'))

    except Exception as e:
        logger.error(f"upsert_dataframe: {e}")
        raise

def repair_d_aggregates(df: pd.DataFrame, timeframes: list[str]):
    try:
        date_obj = df["date"].iloc[0]
        period_map = {
            "D": date_obj,
            "W": pd.Timestamp(date_obj).to_period("W-SUN").start_time.date(),
            "M": pd.Timestamp(date_obj).to_period("M").start_time.date(),
            "Q": pd.Timestamp(date_obj).to_period("Q").start_time.date(),
        }
        with clickhouse_local_connection() as conn:
            conn.execute("SET mutations_sync = 1")
            for tf in timeframes:
                timeframe_start = period_map[tf]
                filter_keys = ", ".join(
                    f"({share_id}, '{timeframe_start}')"
                    for share_id in df["share_id"]
                )
                table_suffix, date_col = ('d', 'date') if tf == 'D' else (f'{tf.lower()}_states', 'timeframe_start')
                delete_query = f'ALTER TABLE timeframe_{table_suffix} DELETE WHERE (share_id, {date_col}) IN ({filter_keys})'
                conn.execute(delete_query)

    except Exception as e:
        logger.exception(f"#repair_timeframe: {e}")


def upsert_daily_aggregates(data: list[dict], table_name: str, share_id: int):
    if not data:
        return

    try:
        with clickhouse_local_connection() as conn:
            keys = [
                f"({row['share_id']}, '{row['date']}')"
                for row in data
            ]

            # Check existing rows
            check_query = f"""
                SELECT 1
                FROM {table_name}
                WHERE (share_id, date) IN ({','.join(keys)})
                LIMIT 1
            """
            is_duplicate = bool(conn.execute(check_query))

            # Delete duplicates
            if is_duplicate:
                # conn.execute("SET mutations_sync = 1")
                # delete_query = f"""
                #     ALTER TABLE {table_name}
                #     DELETE WHERE (share_id, date) IN ({','.join(keys)})
                # """
                # conn.execute(delete_query)
                delete_aggregate_bars(share_id, ['timeframe_d', 'timeframe_w_states', 'timeframe_m_states', 'timeframe_q_states'])

            # Insert rows
            columns = list(data[0].keys())
            insert_query = f"""
                INSERT INTO {table_name}
                ({', '.join(columns)})
                VALUES
            """
            conn.execute(insert_query, data)

    except Exception as e:
        logger.exception(
            f"Failed to upsert {len(data)} rows into {table_name}: {e}"
        )

def delete_aggregate_bars(share_id: int, table_name: str | list[str]) -> bool:
    try:
        with clickhouse_local_connection() as conn:
            conn.execute("SET mutations_sync = 1")
            table_names = [table_name] if isinstance(table_name, str) else table_name
            for table in table_names:
                conn.execute(f'ALTER TABLE {table} DELETE WHERE share_id = {share_id}')
        return True
    except Exception as e:
        logger.exception(
            f"Failed to delete data in table {table_name} for share_id {share_id}: {e}"
        )
        return False

def insert_timeframe_1m(insert_list:list[dict], date_obj: date = None, share_id: int = None, check_duplicates: bool = True):
    if not insert_list:
        return
    try:
        with clickhouse_local_connection() as conn:
            # Check existing rows
            if check_duplicates:
                is_duplicated = conn.execute(f'SELECT 1 FROM timeframe_1m WHERE {f"toDate(datetime) = '{date_obj}'" if date_obj else f"share_id = {share_id}"} LIMIT 1')
                if is_duplicated:
                    repair_1m_aggregates(date_obj, share_id)
            # Insert timeframes in batches
            for i in range(0, len(insert_list), 5000):
                insert_list_chunk = insert_list[i:i + 5000]
                data = [
                    (
                        r["share_id"],
                        r["datetime"],
                        r["time"],
                        r["session"],
                        r["open"],
                        r["high"],
                        r["low"],
                        r["close"],
                        r["volume"],
                        r["vwap"],
                    )
                    for r in insert_list_chunk
                ]
                conn.execute("INSERT INTO timeframe_1m (share_id, datetime, time, session, open, high, low, close, volume, vwap) VALUES", data)

    except Exception as error:
        logger.error(f"#insert_timeframe_1m: {error}")
        raise


def repair_1m_aggregates(date_obj: date = None, share_id: int = None):
    try:
        with clickhouse_local_connection() as conn:
            conn.execute("SET mutations_sync = 1")
            # Delete by date
            if date_obj:
                datetime_start = f'{date_obj} 00:00:00'
                datetime_end = f'{date_obj + timedelta(days=1)} 00:00:00'
                for tf in ['1m', '2m', '3m', '5m', '10m', '15m', '30m', '1h', '2h', '3h', '4h', 'da']:
                    if tf.endswith(('m', 'h')):
                        table_suffix, datetime_col = ('1m', 'datetime') if tf == '1m' else (f'{tf}_states', 'timeframe_start')
                        conn.execute(f"ALTER TABLE timeframe_{table_suffix} DELETE WHERE {datetime_col} >= '{datetime_start}' AND {datetime_col} < '{datetime_end}'")
                    else:
                        conn.execute(f"ALTER TABLE timeframe_{tf}_states DELETE WHERE timeframe_start = '{date_obj}'")
            # Delete by share_id
            else:
                for tf in ['1m', '5m', '15m', '30m', '1h', 'da']:
                    table_suffix = '1m' if tf == '1m' else f'{tf}_states'
                    conn.execute(f"ALTER TABLE timeframe_{table_suffix} DELETE WHERE share_id = {share_id}")
    except Exception as e:
        logger.exception(f"#repair_timeframe: {e}")


def delete_duplicate_keys(table_name: str, share_ids: list[int], date: str):

    if not share_ids:
        return

    ids = ",".join(map(str, share_ids))

    try:
        with clickhouse_local_connection() as conn:
            # Find duplicated keys
            duplicates = conn.query(f"SELECT share_id FROM {table_name} WHERE date = '{date}' AND share_id IN ({ids})").result_rows

            if duplicates:
                dup_ids = ",".join(str(r[0]) for r in duplicates)

                conn.command(f"""
                    ALTER TABLE {table_name} DELETE
                    WHERE date = '{date}'
                    AND share_id IN ({dup_ids})
                """)

    except Exception as e:
        logger.error(f"#delete_duplicate_keys: {e}")


def refresh_shares_info_from_postgres():
    try:
        insert_stmt = f"""
        INSERT INTO fingrep.shares_info
        SELECT
            share_id,
            cik,
            composite_figi,
            homepage_url,
            ipo_date,
            shares_outstanding,
            weighted_shares_outstanding
        FROM postgresql(
            'localhost:{config.DB_PORT}',
            '{config.DB_NAME}',
            'shares_info',
            '{config.DB_USER}',
            '{config.DB_PASSWORD}'
        );
        """
        with clickhouse_local_connection() as conn:
            conn.execute('TRUNCATE TABLE fingrep.shares_info')
            conn.execute(insert_stmt)
    except Exception as e:
        logger.error(f"#refresh_shares_info_from_postgres: {e}")

def refresh_shares_from_postgres():
    try:
        insert_stmt = f"""
        INSERT INTO fingrep.shares
        SELECT
            id,
            name,
            ticker,
            country_id,
            exchange_id,
            industry_id,
            sector_id,
            share_type_id
        FROM postgresql(
            'localhost:{config.DB_PORT}',
            '{config.DB_NAME}',
            'shares',
            '{config.DB_USER}',
            '{config.DB_PASSWORD}'
        );
        """
        with clickhouse_local_connection() as conn:
            conn.execute('TRUNCATE TABLE fingrep.shares')
            conn.execute(insert_stmt)
    except Exception as e:
        logger.error(f"#refresh_shares_info_from_postgres: {e}")


def query_by_template(template: str, template_params: dict):
    try:
        with clickhouse_local_connection() as conn:
            template = template.format(**template_params)
            return conn.execute(template)
    except Exception as e:
        logger.error(f'query_by_template: {e}')