import calendar
import sys
import traceback
from datetime import datetime,date, timezone, timedelta
import inspect
import time
from email.policy import default
from shutil import which

import pandas as pd
import requests
import config
import db_ops
import edgar_service
import finviz
import polygon
import utils
from db_ops import upsert_dataframe, foreign_keys_cache
from edgar_service import get_trading_info
from fundamentals import request_fundamentals, get_period_ending_by_accession_number
from ta_utils import rsi_tv_new_tickers, rsi_tv_existing_tickers
from utils import mandatory_columns, get_utc_date

pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', None)  # To allow the console to use the full width
pd.set_option("future.no_silent_downcasting", True)
from config import logger


def fill_calc_columns(df: pd.DataFrame, table_name: str):
    if table_name == 'balance_sheet':
        # Other non-current assets
        equation = (df.get('current_assets', pd.Series(data=0, index=df.index))
                  - df.get('cash_and_short_term_investments',pd.Series(data=0, index=df.index))
                  - df.get('net_receivables',pd.Series(data=0, index=df.index))
                  - df.get('inventory',pd.Series(data=0, index=df.index)))
        cond = df.get('current_assets', pd.Series(data=0, index=df.index)) > equation
        df.loc[cond, 'other_current_assets'] = equation

        # Other non-current assets
        equation = (df.get('non_current_assets', pd.Series(data=0, index=df.index))
                  - df.get('property_plant_equipment_net', pd.Series(data=0, index=df.index))
                  - df.get('goodwill', pd.Series(data=0, index=df.index))
                  - df.get('intangible_assets', pd.Series(data=0, index=df.index))
                  - df.get('long_term_investments', pd.Series(data=0, index=df.index))
                  - df.get('non_current_deferred_assets', pd.Series(data=0, index=df.index)))
        cond = df.get('non_current_assets', pd.Series(data=0, index=df.index)) > equation
        df.loc[cond, 'other_non_current_assets'] = equation
        
        # Calc other current liabilities
        equation = (df.get('current_liabilities', pd.Series(data=0, index=df.index))
                   - df.get('payables_and_expenses',pd.Series(data=0, index=df.index))
                   - df.get('short_term_debt', pd.Series(data=0, index=df.index)))
        cond = df.get('current_liabilities', pd.Series(data=0, index=df.index)) > equation
        df.loc[cond, 'other_current_liabilities'] = equation

        # Calc other non-current liabilities
        equation = df.get('non_current_liabilities', pd.Series(data=0, index=df.index)) - df.get('long_term_debt', pd.Series(data=0, index=df.index))
        cond = df.get('non_current_liabilities', pd.Series(data=0, index=df.index)) > equation
        df.loc[cond, 'other_non_current_liabilities'] = equation

        return df
    elif table_name == 'cash_flow':
        # Calc other operating activities
        equation = (df.get('operating_cash_flow', pd.Series(data=0, index=df.index))
                    - df.get('operating_net_income', pd.Series(data=0, index=df.index))
                    - df.get('operating_gains_losses', pd.Series(data=0, index=df.index))
                    - df.get('operating_da', pd.Series(data=0, index=df.index))
                    - df.get('deferred_income_tax', pd.Series(data=0, index=df.index))
                    - df.get('share_based_compensation', pd.Series(data=0, index=df.index))
                    - df.get('change_working_capital', pd.Series(data=0, index=df.index)))
        cond = df.get('operating_cash_flow', pd.Series(data=0, index=df.index)) > equation
        df.loc[cond, 'other_operating_activities'] = equation
        # Calc other investing activities
        equation = (df.get('investing_cash_flow', pd.Series(data=0, index=df.index))
                    - df.get('capital_expenditure', pd.Series(data=0, index=df.index))
                    - df.get('investments_PPE', pd.Series(data=0, index=df.index))
                    - df.get('acquisitions_net', pd.Series(data=0, index=df.index))
                    - df.get('purchases_of_investments', pd.Series(data=0, index=df.index)))
        cond = df.get('investing_cash_flow', pd.Series(data=0, index=df.index)) > equation
        df.loc[cond, 'other_investing_activities'] = equation
        # Calc other financing activities
        equation = (df.get('financing_cash_flow', pd.Series(data=0, index=df.index))
                     - df.get('net_debt_issuance', pd.Series(data=0, index=df.index))
                     - df.get('net_common_shares_issued', pd.Series(data=0, index=df.index))
                     - df.get('net_preferred_shares_issued', pd.Series(data=0, index=df.index))
                     - df.get('dividends_paid',pd.Series(data=0, index=df.index)))
        cond = df.get('financing_cash_flow', pd.Series(data=0, index=df.index)) > equation
        df.loc[cond, 'other_financing_activities'] = equation
        # Calc change in cash
        if 'end_cash_balance' in df.columns and 'beginning_cash_balance' in df.columns:
            cond = df['end_cash_balance'].notna() & df['beginning_cash_balance'].notna()
            df.loc[cond, 'change_in_cash'] = df['end_cash_balance'] - df['beginning_cash_balance']

        return df
    else:
        # Calc other operating expenses
        equation = (df.get('operating_expenses', pd.Series(0, index=df.index))
                     - df.get('general_and_administrative_expenses', pd.Series(0, index=df.index))
                     - df.get('depreciation_amortization_depletion', pd.Series(0, index=df.index))
                     - df.get('research_and_development_expenses', pd.Series(0, index=df.index)))
        cond = df.get('operating_expenses', pd.Series(0, index=df.index)) > equation
        df.loc[cond, 'other_operating_expenses'] = equation

        return df


def get_and_insert_fundamentals(share_id: int, ticker: str, cik: str, period_ending: str = None, period_ending_range_search:bool = False, filing_date: str = None):
    counter = 0
    fundamentals_dict = request_fundamentals(ticker=ticker)
    if fundamentals_dict is None:
        time.sleep(3)
        fundamentals_dict = request_fundamentals(ticker=ticker)
        if fundamentals_dict is None:
            logger.error(f"get_and_insert_fundamentals: Skipping fundamentals insertion for {ticker}")
            return False
    # Get date_filed_form_tuples
    edgar_resp = edgar_service.get_company_facts(cik)
    df_edgar = edgar_service.get_position(edgar_resp, 'Assets', date(2019, 12, 31))
    date_filed_tuple = None
    if df_edgar is not None and not df_edgar.empty and {'end', 'filed', 'form'}.issubset(df_edgar.columns):
        date_filed_tuple = [
            (end.date(), pd.to_datetime(filed).date(), form)
            for end, filed, form in zip(df_edgar['end'], df_edgar['filed'], df_edgar['form'])
        ]
    try:
        for statement in ['income_statement', 'cash_flow', 'balance_sheet', 'income_statement_q', 'cash_flow_q', 'balance_sheet_q']:
            if statement not in fundamentals_dict:
                continue
            df = fundamentals_dict[statement]
            # Transpose the DataFrame and reset the index to create a single Date column
            df = df.T.reset_index()
            df.rename(columns={"index": "date"}, inplace=True)
            # Period end search by date-range or date
            if period_ending and period_ending_range_search:
                period_ending = pd.to_datetime(period_ending)
                prev_month_ending_date = (period_ending.replace(day=1) - timedelta(days=1)).date()
                curr_month_ending_date = (period_ending.replace(day=calendar.monthrange(period_ending.year, period_ending.month)[1])).date()
                df = df[(df['date'].dt.date >= prev_month_ending_date) & (df['date'].dt.date <= curr_month_ending_date)]
            elif period_ending and not period_ending_range_search:
                df = df[df['date'].dt.date == pd.Timestamp(period_ending).date()]
            if df.empty:
                continue
            # Currency
            if db_ops.foreign_keys_cache is None:
                db_ops.get_foreign_keys()
            currency_id = db_ops.foreign_keys_cache['currencies'].get(fundamentals_dict['currency'])
            if currency_id is None:
                logger.warning(f"Currency_id is None for currency symbol {fundamentals_dict['currency']} for ticker {ticker}")
            # Add report type, share_id and currency_id columns
            df.loc[:, ['report_type', 'share_id', 'currency_id']] = 'q' if statement.endswith('_q') else 'a', share_id, currency_id
            # Table name
            table_name = statement.rstrip('_q')
            # Filter required columns
            df.columns = df.columns.str.lower()
            pg_columns = utils.pg_tables.get(table_name).keys()
            required_columns = [col for col in pg_columns if col in df.columns]
            df = df[required_columns]
            # Rename columns with db names
            df = df.rename(columns=utils.pg_tables.get(table_name))
            # Query dolt data
            dolt_df = db_ops.get_dolt_statement(ticker, table_name, False, 'Quarter' if statement.endswith('_q') else 'Year')
            dolt_df['date'] = pd.to_datetime(dolt_df['date'])
            # Set index to combine dataframes properly
            dolt_df.set_index(keys=['date','report_type'], inplace=True)
            df.set_index(keys=['date', 'report_type'], inplace=True)
            # Combine dolt data with dataframe
            df = df.combine_first(dolt_df)
            # Add missing rows and columns from df2
            df = (
                pd.concat([df, dolt_df])
                .groupby(level=[0, 1])  # Group by composite index (date, period)
                .first()  # Keep first occurrence (df takes priority)
                .reset_index() # Date formation to prevent an error for forex.request_usd_currency_value
            )
            # Find the oldest row with non-null currency
            oldest_non_null = df[df['currency_id'].notna()].sort_values('date', ascending=True).iloc[0]
            # Apply this currency_id and share_id values to all rows where currency_id is None
            df['currency_id'] = df['currency_id'].fillna(oldest_non_null['currency_id'])
            df['share_id'] = df['share_id'].fillna(oldest_non_null['share_id'])
            # Date must be str for the get_usd_exchange_rate function
            df['date'] = df['date'].dt.strftime('%Y-%m-%d')
            # Stop insertion if any of the mandatory columns is missing
            if any(col not in df.columns for col in mandatory_columns[table_name]):
                continue
            # Remove rows if any of the row's mandatory columns is NaN
            df.dropna(subset=utils.mandatory_columns[table_name], inplace=True)
            if df.empty:
                continue
            # Convert all numeric columns to float, dolt is using decimal type and yfinance float so there is a mismatch
            cols_to_float = df.columns.difference(['date', 'report_type'])
            df[cols_to_float] = df[cols_to_float].astype(float)
            # Fill other columns for balance sheet and cash flow statements
            df = fill_calc_columns(df=df, table_name=table_name)
            # Calc report period
            df[['report_period_id', 'filing_date']] = df.apply(lambda row: calc_report_period_id_and_filing_date(row, date_filed_tuple, filing_date), axis=1)
            # Remove rows that aren't equal to the filing date
            if filing_date:
                df = df[df['filing_date'].dt.date == pd.Timestamp(filing_date).date()]
            if df.empty:
                continue
            # Magnification
            monetary_columns = df.columns.difference(utils.non_monetary_columns)
            df[monetary_columns] = df[monetary_columns].mul(10000).round(0)
            # Important for ratios trigger function
            df.sort_values(by=['date'], inplace=True, ascending=True)
            if upsert_dataframe(df, table_name): counter += 1
    except Exception as e:
        logger.error(f"get_and_insert_fundamentals - Error preparing for insert fundamentals for ticker {ticker}: {e}\n{traceback.format_exception(e)}")
        logger.error(f"Dataframe for table_name = {table_name}:\n{df}")
    finally:
        return counter % 3 == 0 if counter != 0 else False


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


def get_grouped_daily_bars(date_str: str) -> pd.DataFrame:
        data = polygon.request_grouped_daily_bars(date=date_str)
        if data['resultsCount'] == 0:
            return pd.DataFrame()
        df = pd.DataFrame(data['results'])
        # Checks resultsCount and the actual results array length
        row_count = len(df)
        if row_count < 9000:
            logger.critical(f"Request_grouped_daily_bars has returned {row_count} and expected at least 9000. Exiting the script.",stack_info=True)
            sys.exit(1)
        # Removes rows that contain at least one NaN OHLC value
        tickers_with_nan = df.loc[df[['o', 'h', 'l', 'c']].isna().any(axis=1), 'T'].tolist()
        if tickers_with_nan:
            logger.warning(f": The following tickers had at least one NaN OHLC value on the date {get_utc_date(days=config.DAYS)}: ",
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


def insert_grouped_daily_bars(df):
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
    if config.insert_fundamentals and cik is not None and ticker_id is not None and shares_data.get('share_type_id') not in(6, 8):
        get_and_insert_trading_info(cik=cik, share_id=ticker_id, date=date(2019, 12, 31))
        get_and_insert_fundamentals(share_id=ticker_id, ticker=ticker, cik=cik)


# Separates data for shares and share_info tables
def extract_ticker_details_v3(ticker_details, finviz_data):
    if foreign_keys_cache is None:
        db_ops.get_foreign_keys()

    ticker_data = {
        'ticker': ticker_details.get('ticker'),
        'cik': ticker_details.get('cik'),
        'name': utils.remove_stock_suffix(ticker_details.get('name')),
        'exchange_id': db_ops.foreign_keys_cache['exchanges'].get(ticker_details.get('primary_exchange')),
        'homepage_url': ticker_details.get('homepage_url'),
        'ipo_date': ticker_details.get('list_date'),
        'share_type_id': db_ops.foreign_keys_cache['share_types'].get(ticker_details.get('type')),
        'composite_figi': ticker_details.get('composite_figi'),
        'shares_outstanding': ticker_details.get('share_class_shares_outstanding'),
        'weighted_shares_outstanding': ticker_details.get('weighted_shares_outstanding'),
        'sector_id': db_ops.foreign_keys_cache['sectors'].get(finviz_data.get('sector')),
        'industry_id': db_ops.foreign_keys_cache['industries'].get(finviz_data.get('industry')),
        'country_id': db_ops.foreign_keys_cache['countries'].get(finviz_data.get('country'))
    }

    shares = {key: value for key, value in ticker_data.items() if
              key not in ['weighted_shares_outstanding', 'ipo_date', 'shares_outstanding', 'cik', 'homepage_url', 'composite_figi']}
    shares_info = {key: ticker_data[key] for key in
                   ['weighted_shares_outstanding', 'ipo_date', 'shares_outstanding', 'cik', 'homepage_url', 'composite_figi']}

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

def calc_report_period_id_and_filing_date(row, dates_tuple, filing_date):
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
    filing_date = filing_date if filing_date else find_nearest(row_date.date(), dates_tuple)

    return pd.Series([report_period_id, pd.to_datetime(filing_date)])


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

# Update fundamentals for existing shares
def update_fundamentals():
    # Update latest financials
    df_latest_filings = edgar_service.get_latest_filings()
    if df_latest_filings is not None and not df_latest_filings.empty:
        # Partially insert key financials
        #df_latest_filings = df_latest_filings.head(20) #todo don't forget to remove this in prod
        df_cik = db_ops.get_ids_by_cik(df_latest_filings['cik'].unique().tolist())
        df_latest_filings = pd.merge(df_latest_filings,df_cik, how='left', on='cik')
        df_latest_filings.loc[:,['revenue', 'eps', 'net_income', 'avg_shares_outstanding', 'date', 'report_period_id', 'partially_inserted']] = (
            df_latest_filings.apply(edgar_service.update_income_positions, axis=1))
        df_partial_insert = df_latest_filings[df_latest_filings['partially_inserted'] == True]
        is_inserted = db_ops.upsert_dataframe(df_partial_insert[df_partial_insert.columns.difference(['cik', 'accession_number', 'fully_inserted'])],'income_statement')
        if is_inserted:
            df_latest_filings['partially_inserted'] = df_latest_filings['cik'].map(df_partial_insert.set_index('cik')['partially_inserted']).fillna(False)
        # Upsert latest filings
        df_latest_filings.drop(columns=['revenue', 'eps', 'net_income', 'report_type', 'date', 'report_period_id', 'avg_shares_outstanding', 'share_id'], inplace=True)
        db_ops.upsert_latest_filings(df=df_latest_filings)
    else:
        logger.warning('update_fundamentals: no latest filings to update')
    # Delete filings older than a month
    db_ops.delete_fillings_older_than_month()
    # Update fundamentals
    df_full_insert = db_ops.get_filings_for_full_insert()
    if not df_full_insert.empty:
        df_full_insert['period_ending'] = df_full_insert.apply(lambda row: get_period_ending_by_accession_number(accession_number=row['accession_number']),axis=1)
        df_full_insert['fully_inserted'] = df_full_insert.apply(lambda row: get_and_insert_fundamentals(share_id=row['id'],ticker=row['ticker'],cik=row['cik'],period_ending=row['period_ending'],period_ending_range_search=True,filing_date=row['filing_date']),axis=1)
        df_full_insert['full_insert_attempt_date'] = date.today().strftime('%Y-%m-%d')
        df_latest_filings = df_full_insert[df_full_insert['fully_inserted'] == True]
        df_latest_filings = df_latest_filings[['accession_number', 'cik', 'fully_inserted', 'full_insert_attempt_date', 'filing_date']]
        db_ops.upsert_latest_filings(df_latest_filings, upsert_fully_inserted_and_attempt_date=True)


def get_prev_grouped_daily_bars():
    for i in range(1,10):
        prev_date = get_utc_date(days=config.DAYS + i, as_str=False)
        if prev_date.weekday() in (5, 6):
            continue
        df = get_grouped_daily_bars(date_str=prev_date.strftime("%Y-%m-%d"))
        if not df.empty:
            return df
    logger.critical(f"Couldn't obtain previous day grouped daily data. Exiting the script.", exc_info=True)
    sys.exit(1)

def get_all_tickers(date_str: str):
    return polygon.request_all_tickers(date=date_str)

