import traceback
from datetime import datetime,date, timezone, timedelta
import inspect
import time
import pandas as pd
import requests
from cffi.cffi_opcode import PRIM_FLOAT

import config
import db_ops
import edgar
import finviz
import forex
import polygon
import utils
from db_ops import upsert_dataframe, get_foreign_keys, foreign_keys_cache
from edgar import get_trading_info
from fundamentals import get_fundamentals
from ta_utils import rsi_tv_new_tickers, rsi_tv_existing_tickers
from utils import mandatory_columns
pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', None)  # To allow the console to use the full width
pd.set_option("future.no_silent_downcasting", True)
from config import logger


def fill_calc_columns(df: pd.DataFrame, table_name: str):
    if table_name == 'balance_sheet':
        # Other non-current assets
        equation = (df.get('current_assets', 0)
                  - df.get('cash_and_short_term_investments',0)
                  - df.get('net_receivables',0)
                  - df.get('inventory',0))
        cond = df.get('current_assets', 0) > equation
        df.loc[cond, 'other_current_assets'] = equation

        # Other non-current assets
        equation = (df.get('non_current_assets', 0)
                  - df.get('property_plant_equipment_net',0)
                  - df.get('goodwill',0)
                  - df.get('intangible_assets',0)
                  - df.get('long_term_investments',0)
                  - df.get('non_current_deferred_assets',0))
        cond = df.get('non_current_assets', 0) > equation
        df.loc[cond, 'other_non_current_assets'] = equation
        
        # Calc other current liabilities
        equation = (df.get('current_liabilities', 0)
                   - df.get('payables_and_expenses',0)
                   - df.get('short_term_debt', 0))
        cond = df.get('current_liabilities', 0) > equation
        df.loc[cond, 'other_current_liabilities'] = equation

        # Calc other non-current liabilities
        equation = df.get('non_current_liabilities', 0) - df.get('long_term_debt', 0)
        cond = df.get('non_current_liabilities', 0) > equation
        df.loc[cond, 'other_non_current_liabilities'] = equation

        return df
    elif table_name == 'cash_flow':
        # Calc other operating activities
        equation = (df.get('operating_cash_flow', 0)
                    - df.get('operating_net_income', 0)
                    - df.get('operating_gains_losses', 0)
                    - df.get('operating_da', 0)
                    - df.get('deferred_income_tax', 0)
                    - df.get('share_based_compensation', 0)
                    - df.get('change_working_capital', 0))
        cond = df.get('operating_cash_flow', 0) > equation
        df.loc[cond, 'other_operating_activities'] = equation
        # Calc other investing activities
        equation = (df.get('investing_cash_flow', 0)
                    - df.get('capital_expenditure', 0)
                    - df.get('investments_PPE', 0)
                    - df.get('acquisitions_net', 0)
                    - df.get('purchases_of_investments', 0))
        cond = df.get('investing_cash_flow', 0) > equation
        df.loc[cond, 'other_investing_activities'] = equation
        # Calc other financing activities
        equation = (df.get('financing_cash_flow',0)
                     - df.get('net_debt_issuance',0)
                     - df.get('net_common_shares_issued',0)
                     - df.get('net_preferred_shares_issued',0)
                     - df.get('dividends_paid',0))
        cond = df.get('financing_cash_flow',0) > equation
        df.loc[cond, 'other_financing_activities'] = equation
        # Calc change in cash
        if 'end_cash_balance' in df.columns and 'beginning_cash_balance' in df.columns:
            cond = df['end_cash_balance'].notna() & df['beginning_cash_balance'].notna()
            df.loc[cond, 'change_in_cash'] = df['end_cash_balance'] - df['beginning_cash_balance']

        return df
    else:
        # Calc other operating expenses
        equation = (df.get('operating_expenses',0)
                     - df.get('general_and_administrative_expenses',0)
                     - df.get('depreciation_amortization_depletion',0)
                     - df.get('research_and_development_expenses',0))
        cond = df.get('operating_expenses',0) > equation
        df.loc[cond, 'other_operating_expenses'] = equation

        return df


def get_and_insert_fundamentals(share_id: int, ticker: str, cik: str, period_ending: str = None, filling_date: str = None)->bool:
    counter = 0
    fundamentals_dict = get_fundamentals(ticker=ticker)
    if fundamentals_dict is None:
        time.sleep(3)
        fundamentals_dict = get_fundamentals(ticker=ticker)
        if fundamentals_dict is None:
            logger.error(f"get_and_insert_fundamentals: Skipping fundamentals insertion for {ticker}")
            return
    # Get date_filed_form_tuples
    edgar_resp = edgar.get_company_facts(cik)
    df_edgar = edgar.get_position(edgar_resp, 'Assets', date(2019, 12, 31))
    date_filed_tuple = None
    if df_edgar is not None and not df_edgar.empty:
        # Convert all values to datetime.date
        date_filed_tuple = [
            (end.date(), pd.to_datetime(filed).date(), form)
            for end, filed, form in zip(df_edgar['end'], df_edgar['filed'])
        ]
    try:
        for statement in ['income_statement', 'cash_flow', 'balance_sheet', 'income_statement_q', 'cash_flow_q', 'balance_sheet_q']:
            if statement not in fundamentals_dict:
                continue
            df = fundamentals_dict[statement]
            # Transpose the DataFrame and reset the index to create a single Date column
            df = df.T.reset_index()
            df.rename(columns={"index": "date"}, inplace=True)
            if period_ending:
                df = df[df['date'].dt.date == pd.Timestamp(period_ending).date()]
            if df.empty:
                continue
            # Add period, share_id and currency columns
            df.loc[:, ['report_type', 'share_id', 'currency']] = 'q' if statement.endswith('_q') else 'a', share_id, fundamentals_dict['currency']
            # Table name
            table_name = statement.rstrip('_q')
            # Filter required columns
            df.columns = df.columns.str.lower()
            pg_columns = utils.pg_tables.get(table_name).keys()
            required_columns = [col for col in pg_columns if col in df.columns]
            df = df[required_columns]
            # Rename columns with db names
            df = df.rename(columns=utils.pg_tables.get(statement.rstrip('_q')))
            # Date formation to prevent an error for forex.request_usd_currency_value
            df['date'] = df['date'].dt.strftime('%Y-%m-%d')
            # Stop insertion if any of the mandatory columns is missing
            if any(col not in df.columns for col in mandatory_columns[table_name]):
                continue
            # Remove rows if any of the row's mandatory columns is NaN
            df.dropna(subset=utils.mandatory_columns[table_name], inplace=True)
            if df.empty:
                continue
            # Fill other columns for balance sheet and cash flow statements
            df = fill_calc_columns(df=df, table_name=table_name)
            # Currency conversion
            df['usd_exc'] = df.apply(forex.get_usd_exchange_rate, axis=1)
            monetary_columns = df.columns.difference(utils.non_monetary_columns)
            df[monetary_columns] = df[monetary_columns].div(df['usd_exc'], axis=0).mul(10000).round()
            df.drop(columns=['usd_exc', 'currency'], inplace=True)
            # Calc report period
            df[['report_period_id', 'filing_date']] = df.apply(lambda row: calc_report_period_id_and_filing_date(row, date_filed_tuple), axis=1)
            # Remove rows that aren't equal to the filing date
            if filling_date:
                df = df[df['date'].dt.date == pd.Timestamp(filling_date).date()]
            if df.empty:
                continue
            # Important for ratios trigger function
            df.sort_values(by=['date'], inplace=True, ascending=True)
            if upsert_dataframe(df, table_name): counter += 1
        # Check if all annual and quarterly are inserted
        return counter == 6

    except Exception as e:
        logger.error(f"get_and_insert_fundamentals - Error preparing for insert fundamentals for ticker {ticker}: {e}\n{traceback.format_exception(e)}")
    finally:
        return False


def get_and_insert_trading_info(cik: str, share_id: int, date: datetime.date):
    method_name = inspect.currentframe().f_code.co_name

    try:
        df = get_trading_info(cik=cik, date=date)
        if df is not None:
            df.reset_index(inplace=True)
            df.rename(columns={'end': 'date'}, inplace=True)
            df.loc[:, ['share_id']] = share_id
            upsert_dataframe(df, 'trade_info')
    except Exception as e:
        logger.error(
            f"{method_name}: Error occurred while trying to rename and reindex data frame for upsert into database for CIK {cik} "
            f"and share ID {share_id}: {e}")


def call_and_update_market_breadth(date):
    db_ops.update_market_breadth(date)


def get_grouped_daily_bars():

        data = polygon.request_grouped_daily_bars()
        df = pd.DataFrame(data['results'])
        # Checks resultsCount and the actual results array length
        method_name = inspect.currentframe().f_code.co_name
        row_count = len(df)
        if row_count < 8000:
            logger.warning(f"{method_name}: request_grouped_daily_bars has returned {row_count}.")
        # Removes rows that contain at least one NaN OHLC value
        tickers_with_nan = df.loc[df[['o', 'h', 'l', 'c']].isna().any(axis=1), 'T'].tolist()
        if tickers_with_nan:
            logger.warning(f"{method_name}: The following tickers had at least one NaN OHLC value on the date ", datetime.utcnow(), ":",
                  tickers_with_nan)
        # Volume column conversion to integer
        df['v'] = df['v'].astype(int)
        return df


def get_and_insert_aggregated_bars(ticker, ticker_id, date_from, limit):

    data = polygon.request_aggregate_daily_bars(ticker, date_from, limit)
    df_aggregated_daily = pd.DataFrame(data['results'])

    # Prepare for insert
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
    df_aggregated_daily[utils.magnified_columns_new] = df_aggregated_daily[utils.magnified_columns_new] * 10000

    # Insert into database
    db_ops.upsert_dataframe_v2(df_aggregated_daily, 'd_timeframe')


def rename_and_insert_grouped_daily_bars(df):
    df['date'] = pd.to_datetime(df['t'], unit='ms').dt.date
    df = df.drop(['T', 'n', 't'], axis=1)
    df['id'] = df['id'].astype(int)
    df.rename(
        columns={'v': 'volume', 'o': 'open', 'c': 'close', 'h': 'high', 'l': 'low', 'id': 'share_id', 'vw': 'vwap'},
        inplace=True)
    df[utils.magnified_columns_existing] = df[utils.magnified_columns_existing] * 10000
    db_ops.upsert_dataframe_v2(df, 'd_timeframe')


def get_new_ticker_data_and_insert(ticker, finviz_df):
    method_name = inspect.currentframe().f_code.co_name

    try:
        ticker_data = polygon.request_ticker_details_v3(ticker)
    except requests.RequestException:
        logger.warning(f"{method_name} - Couldn't obtain ticker details.")
        return

    finviz_ticker_df = finviz_df.query("ticker == @ticker")
    if not finviz_ticker_df.empty:
        finviz_data = {
            'sector': finviz_ticker_df['sector'].values[0],
            'industry': finviz_ticker_df['industry'].values[0],
            'country': finviz_ticker_df['country'].values[0]
        }
    else:
        finviz_data = finviz.get_sic(ticker)
    shares_data, shares_info_data = extract_ticker_details_v3(ticker_data, finviz_data)
    # Check if ticker type is allowed
    if shares_data.get("share_type_id") is None:
        return
    elif shares_data.get("share_type_id") not in utils.allowed_share_type_ids:
        db_ops.insert_banned_ticker(ticker)
        return
    ticker_id = db_ops.insert_new_ticker(shares_data, shares_info_data)
    # Starter plan required for 2+ years historical data
    date_from = datetime.utcnow().replace(tzinfo=timezone.utc).date() - timedelta(days=365 * config.YEARS)
    get_and_insert_aggregated_bars(ticker, ticker_id, date_from, 5000)
    # Fundamental data and trade info
    cik = shares_info_data.get('cik')
    if config.fundamentals and cik is not None and ticker_id is not None and shares_data.get('share_type_id') not in(6, 8):
        get_and_insert_trading_info(cik=cik, share_id=ticker_id, date=date(2019, 12, 30))
        get_and_insert_fundamentals(share_id=ticker_id, ticker=ticker, cik=cik)


# Separates data for shares and share_info tables
def extract_ticker_details_v3(ticker_details, finviz_data):
    foreign_keys = db_ops.get_foreign_keys()
    ticker_data = {
        'ticker': ticker_details.get('ticker'),
        'cik': ticker_details.get('cik'),
        'name': utils.remove_stock_suffix(ticker_details.get('name')),
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


def update_rsi_existing_tickers():
    df_last_100 = db_ops.get_last_100()
    df_last_100.sort_values(by='date', ascending=True, inplace=True)
    df_last_100['rsi'] = df_last_100.groupby('share_id', as_index=False).apply(
        lambda group: rsi_tv_existing_tickers(group), include_groups=False).reset_index(level=0, drop=True)
    utc_now = datetime.utcnow().replace(tzinfo=timezone.utc).date() - timedelta(days=config.DAYS)
    df_last_100 = df_last_100.query("date == @utc_now")
    df_last_100 = df_last_100.drop(columns=['close', 'high', 'low'])
    df_last_100.dropna(subset=['rsi'], inplace=True)
    update_data = [(row['rsi'], row['share_id'], row['date'])
                   for index, row in df_last_100.iterrows()]
    # Update db
    db_ops.update_rsi(update_data)


def get_splits():
    try:
        res = polygon.request_splits()
        arr = [entry['ticker'] for entry in res]
        return arr
    except TypeError as e:
        logger.error(f"get_splits - TypeError {e}")
    except requests.RequestException as e:
        logger.error(f"get_splits - RequestException {e}")

def calc_report_period_id_and_filing_date(row, dates_tuple):
    # Report period calculation
    row_date = pd.to_datetime(row['date'])
    report_type = row['report_type']
    year = row_date.year
    report_period = None
    if report_type == 'a':  # Annual
        year_start = pd.Timestamp(f"{year}-01-01")
        is_curr = (row_date - year_start).days / 365 > 0.5
        report_period = f"{year}" if is_curr else f"{year - 1}"

    elif report_type == 'q':  # Quarterly

        q1_start = pd.Timestamp(f"{year}-01-01")
        q1_end = pd.Timestamp(f"{year}-03-31")
        q2_start = pd.Timestamp(f"{year}-04-01")
        q2_end = pd.Timestamp(f"{year}-06-30")
        q3_start = pd.Timestamp(f"{year}-07-01")
        q3_end = pd.Timestamp(f"{year}-09-30")
        q4_start = pd.Timestamp(f"{year}-10-01")
        q4_end = pd.Timestamp(f"{year}-12-31")

        if q1_start <= row_date <= q1_end:
            is_curr = (row_date - q1_start).days / (q1_end - q1_start).days > 0.5
            report_period = f"{year}q1" if is_curr else f"{year - 1}q4"
        elif q2_start <= row_date <= q2_end:
            is_curr = (row_date - q2_start).days / (q2_end - q2_start).days > 0.5
            report_period = f"{year}q2" if is_curr else f"{year}q1"
        elif q3_start <= row_date <= q3_end:
            is_curr = (row_date - q3_start).days / (q3_end - q3_start).days > 0.5
            report_period = f"{year}q3" if is_curr else f"{year}q2"
        elif q4_start <= row_date <= q4_end:
            is_curr = (row_date - q4_start).days / (q4_end - q4_start).days > 0.5
            report_period = f"{year}q4" if is_curr else f"{year}q3"
    # Get report period id
    report_period_id = utils.report_periods[report_period]
    # Find filing date by nearest or matching period ending date
    filing_date = find_nearest(row_date.date(), dates_tuple)

    return pd.Series([report_period_id, filing_date])


def find_nearest(period_ending: date, dates_tuple):
    if not dates_tuple:
        return None
    min_index = None
    min_diff = float('inf')
    for i in range(0, len(dates_tuple)):
        dt = dates_tuple[i][0]
        diff = abs((period_ending - dt).days)
        if diff == 0:
            return dates_tuple[i][1]
        elif min_diff > diff:
            min_index = i
            min_diff = diff

    return dates_tuple[min_index][1] if min_diff < 40 else None
