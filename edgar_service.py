import re
import time
import datetime
from datetime import timedelta
#from distutils.command.install import value

import edgar._filings
from edgar import get_filings, set_identity, get_by_accession_number
import pandas as pd
import requests
from bs4 import BeautifulSoup
from edgar.core import edgar_data_dir

import config
import forex
import fundamentals
pd.set_option('future.no_silent_downcasting', True)
import db_ops
import utils
# Set up logging
from config import logger
# Set identity for edgar
set_identity('milosfxc@gmail.com')


def get_latest_filings():
    # Get max date from latest filings
    current_date = datetime.date.today() - timedelta(days=config.DAYS)
    latest_filings = db_ops.get_latest_filings_by_max_filing_date()
    df_db = None
    if latest_filings:
        df_db = pd.DataFrame(latest_filings)
        max_filing_date = pd.to_datetime(df_db.loc[0, 'filing_date']).date()
    else:
        max_filing_date = current_date
    delta = (current_date - max_filing_date).days + 1
    # Get all filings from latest filing date to current date
    df_edgar = None
    for i in range(0, delta):
        fd = max_filing_date + timedelta(days=i)
        if fd.weekday() not in [5, 6]:
            filings = get_filings(form=['10-K', '10-Q', '20-F', '6-K'], filing_date=fd.strftime('%Y-%m-%d'), amendments=False)
            if filings:
                df_edgar = pd.concat([df_edgar, filings.to_pandas()], ignore_index=True) if df_edgar is not None else filings.to_pandas()
    if df_edgar is not None and not df_edgar.empty:
        # Replace values in form column, 10-A = 1, 10-Q = 2
        df_edgar['report_type'] = df_edgar['form'].replace(utils.form_report_type)
        # Drop unnecessary columns
        df_edgar.drop(columns=['company', 'form'], inplace=True)
        df_edgar[['partially_inserted', 'fully_inserted']] = False
        # Remove already partially inserted and inserted rows
        if df_db is not None and not df_db.empty:
            partially_inserted_filings = df_db[df_db['partially_inserted'] == True]['accession_number'].tolist()
            fully_inserted_filings = df_db[df_db['fully_inserted'] == True]['accession_number'].tolist()
            df_edgar = df_edgar[~df_edgar['accession_number'].isin(fully_inserted_filings + partially_inserted_filings)]
            return df_edgar
        else:
            return df_edgar
    return None


def update_income_positions(row):
    accession_number = row['accession_number']
    cols = ['revenue', 'eps', 'net_income', 'avg_shares_outstanding', 'date', 'report_period_id', 'partially_inserted']
    filing = get_by_accession_number(accession_number=accession_number)
    if filing is None or not hasattr(filing, 'period_of_report'):
        logger.info(f"Couldn't get filing or period_or_report for the filing accession number: {accession_number}")
        return pd.Series(data=[None, None, None, None, None, None, False],index=cols)
    # Period ending
    period_ending = filing.period_of_report
    if period_ending is None:
        logger.info(f"period_or_report is None for the filing accession number: {accession_number}")
        return pd.Series(data=[None, None, None, None, None, None, False],index=cols)
    # Retrieve the income statement
    try:
        df_filing = filing.obj().financials.get_income_statement().get_dataframe()
    except Exception as e:
        logger.info(f"Unexpected error processing filing {accession_number}: {str(e)}", exc_info=True)
        return pd.Series(data=[None, None, None, None, None, None, False],index=cols)
    if df_filing.empty:
        return pd.Series(data=[None, None, None, None, None, None, False],index=cols)
    # Convert string to numeric
    df_filing.iloc[:, 0] = pd.to_numeric(df_filing.iloc[:, 0], errors='coerce')
    # Dataframe that contains revenue word in concept column rows
    df_revenue = df_filing[df_filing['concept'].str.contains('revenue', case=False, na=False)]
    # Retrieve the max value
    max_val = df_revenue.iloc[:,0].max()
    # Filter rows where the first column matches max_val
    df_max = df_revenue[df_revenue.iloc[:, 0] == max_val]
    # Ensure df_max is not empty before accessing values
    if df_max.empty:
        logger.info(f"df_max is empty, no matching max value found for accession number: {accession_number}")
        return pd.Series(data=[None, None, None, None, None, None, False],index=cols)
    # Extract concept with max value safely
    max_val_concept = df_max['concept'].values[0]
    # Filter rows with the same concept as max_val_concept
    df_revenue = df_filing[df_filing['concept'] == max_val_concept]
    # Calc revenue safely to prevent IndexError
    revenue = calc_revenue(df_revenue)
    if revenue: revenue = revenue * 10000
    # Calc eps safely to prevent IndexError
    df_eps = df_filing[df_filing['concept'] == 'us-gaap_EarningsPerShareBasic']
    eps = df_eps.iloc[0, 0] * 10000 if not df_eps.empty else None
    # Calc net income
    df_net_income = df_filing[df_filing['concept'] == 'us-gaap_NetIncomeLoss']
    net_income = df_net_income.iloc[0, 0] * 10000 if not df_net_income.empty else None
    # Calc average shares outstanding
    df_average_shares = df_filing[df_filing['concept'] == 'us-gaap_WeightedAverageNumberOfSharesOutstanding']
    average_shares = df_average_shares.iloc[0, 0] * 10000 if not df_average_shares.empty else None
    # Calc report_period_id
    report_period_id = fundamentals.calc_report_period_id(period_ending, row['report_type'])
    # Returns revenue, eps, net income, period ending and is_insertable
    return pd.Series(data=[revenue, eps, net_income, average_shares, period_ending, report_period_id, True],index=cols)


def calc_revenue(revenues):
    # Remove NaN values to prevent arithmetic errors
    revenues = revenues.iloc[:,0].dropna().tolist()
    total_revenue = sum(revenues)
    list_size = len(revenues)
    # Returns total revenue even if we have two different revenues with the same value
    if list_size == 2:
        return total_revenue
    for i in range(0, list_size):
        if (total_revenue - revenues[i]) == revenues[i]: # checks if revenue[i] is aggregate position
            return revenues[i]
    return total_revenue # returns total revenue if none of the revenues is aggregate


def get_trading_info(cik: str, date: datetime.date):
    """
    Get trading information by merging EntityCommonStockSharesOutstanding and CommonStockSharesOutstanding if possible.

    Parameters:
    cik (str): The Central Index Key (CIK) of the company.

    Returns:
    pd.DataFrame: DataFrame with the trading information if successful, None otherwise.
    """
    response = get_company_facts(cik=cik, retries=3, delay=3)
    if response is None:
        return None
    df_entity = get_position(response=response, position_name='EntityCommonStockSharesOutstanding', date=date)
    df_common = get_position(response=response, position_name='CommonStockSharesOutstanding', date=date)

    if df_entity is None and df_common is None:
        return None

    if df_entity is not None:
        df_entity.rename(columns={'end': 'date'}, inplace=True)
        df_entity.set_index('date', inplace=True)
        df_entity = df_entity[['EntityCommonStockSharesOutstanding']]
        if df_common is None:
            df_entity.rename(columns={'EntityCommonStockSharesOutstanding': 'common_shares_outstanding'},
                             inplace=True)
            return df_entity

    if df_common is not None:
        df_common.rename(columns={'end': 'date'}, inplace=True)
        df_common.set_index('date', inplace=True)
        df_common = df_common[['CommonStockSharesOutstanding']]
        if df_entity is None:
            df_common.rename(columns={'CommonStockSharesOutstanding': 'common_shares_outstanding'},
                             inplace=True)
            return df_common

    df = pd.merge(df_entity, df_common, left_index=True, right_index=True, how='outer')
    df['common_shares_outstanding'] = (
        df[['EntityCommonStockSharesOutstanding', 'CommonStockSharesOutstanding']].max(axis=1))
    df = df[~df.index.duplicated(keep='first')]
    df = df[['common_shares_outstanding']]
    return df


def get_company_facts(cik: str, retries: int = 3, delay: int = 5):
    """
    Fetch company facts from SEC EDGAR API for a given CIK.

    Parameters:
    cik (str): The Central Index Key (CIK) of the company.
    retries (int): Number of retries before giving up.
    delay (int): Delay between retries in seconds.

    Returns:
    dict: JSON response containing company facts if successful, None otherwise.
    """
    cik = str(cik).zfill(10)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=utils.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                logger.warning(f"Error getting company facts for CIK {cik}: {e}")
    return None


def get_position(response, position_name, date: datetime.date):
    """
    Extract position data from the company facts response.

    Parameters:
    response (dict): The JSON response containing company facts.
    position_name (str): The position name to extract from the response.

    Returns:
    pd.DataFrame: DataFrame with the extracted position data if successful, None otherwise.
    """
    for position in utils.edgar_company_facts_positions_path[position_name]:
        if response and position in response:
            response = response[position]
        else:
            return None
    df = pd.DataFrame(response)
    df['end'] = pd.to_datetime(df['end'])
    df = df.query("end >= @date")
    df = df.rename(columns={'val': position_name}, inplace=False)
    return df


def request_latest_fillings(form: str = '10-K', retries: int = 3, delay: int = 5):
    """
    Fetch company facts from SEC EDGAR API for a given CIK.

    Parameters:
    form (str): Form filling type
    retries (int): Number of retries before giving up.
    delay (int): Delay between retries in seconds.

    Returns:
    requests.models.Response: Response containing latest fillings if successful, None otherwise.
    """
    url = f"https://www.sec.gov/cgi-bin/browse-edgar?company=&CIK=&type={form}&owner=include&count=100&action=getcurrent"
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=utils.headers)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                logger.error(f"get_latest_fillings - {e}")
    return None


def get_latest_fillings():
    ans = []
    response = request_latest_fillings()
    if response:
        # Parse the HTML response using BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        filings_table = soup.find_all('table')[6]
        rows = filings_table.find_all('tr')
        cik, accepted = None, None
        for row in rows[1:]:
            cells = row.find_all('td')
            if len(cells) == 3:
                description = cells[2]
                anchor = description.find('a')
                if anchor and anchor['href']:
                    cik_match = re.search(r'CIK=(\d{10})', anchor['href'])
                    if cik_match:
                        cik = cik_match.group(1)
                        continue

            if len(cells) == 6:
                date_part, time_part = cells[3].get_text(separator=' ').split()
                accepted = date_part + ' ' + time_part

                if cik and accepted:
                    ans.append((cik, accepted))
                cik, filling_date = None, None
        if not ans:
            logger.error(f"get_latest_fillings: The method may not be working because it hasn't collected any fillings.")
        return ans


def get_submissions(cik: str, retries: int = 3, delay: int = 5):
    """
    Fetch company submissions from SEC EDGAR API for a given CIK.

    Parameters:
    cik (str): The Central Index Key (CIK) of the company.
    retries (int): Number of retries before giving up.
    delay (int): Delay between retries in seconds.

    Returns:
    dict: JSON response containing company submissions if successful, None otherwise.
    """
    cik = cik.zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=utils.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                logger.critical(f"Error getting company facts for CIK {cik}: {e}")
    return None

def get_ticker_by_cik(cik: str):
    ans = get_submissions(cik)
    if ans:
        try:
            return ans['tickers'][0]
        except TypeError as e:
            logger.warning(f"get_ticker_by_cik - for cik {cik}: {e}")
            return None


# These two functions should be used for earnings calendar
# def update_latest_filings():
#     # Get max date from latest filings
#     current_date = datetime.date.today()
#     latest_filings = db_ops.get_latest_filings_by_max_filing_date()
#     df = None
#     if latest_filings:
#         df = pd.DataFrame(latest_filings)
#         max_filing_date = pd.to_datetime(df.loc[0, 'filing_date']).date()
#     else:
#         max_filing_date = current_date
#     delta = (current_date - max_filing_date).days + 1
#     # Get all filings from latest filing date to current date
#     df_edgar = None
#     for i in range(0, delta):
#         fd = max_filing_date + timedelta(days=i)
#         if fd.weekday() not in [5, 6]:
#             filings = get_filings(form=['10-K', '10-Q', '20-F', '6-K'], filing_date=fd.strftime('%Y-%m-%d'), amendments=False)
#             if filings:
#                 df_edgar = pd.concat([df_edgar, filings.to_pandas()], ignore_index=True) if df_edgar is not None else filings.to_pandas()
#     if df_edgar is not None and not df_edgar.empty:
#         # Replace values in form column, 10-A = 1, 10-Q = 2
#         df_edgar['report_type_id'] = df_edgar['form'].replace(utils.form_report_type_id)
#         # Drop unnecessary columns
#         df_edgar.drop(columns=['company', 'form'], inplace=True)
#         df_edgar['partially_inserted'] = False
#         df_edgar['inserted'] = False
#         # Remove already partially inserted rows
#         if df is not None and not df.empty:
#             partially_inserted_filings = df[df['partially_inserted'] == True]['accession_number'].tolist()
#             df_edgar = df_edgar[~df_edgar['accession_number'].isin(partially_inserted_filings)]
#         # Get income stmt positions
#         df_edgar[['revenue', 'eps', 'net_income', 'period_ending', 'partially_inserted']] = df_edgar.apply(update_income_positions, axis=1)
#         # Get currency
#         # Insert into db

# Calculates revenue, eps and net income
# def calc_revenue(revenues):
#     # Remove NaN values to prevent arithmetic errors
#     revenues = revenues.iloc[:,0].dropna().tolist()
#     total_revenue = sum(revenues)
#     list_size = len(revenues)
#     # Returns total revenue even if we have two different revenues with the same value
#     if list_size == 2:
#         return total_revenue
#     for i in range(0, list_size):
#         if (total_revenue - revenues[i]) == revenues[i]: # checks if revenue[i] is aggregate position
#             return revenues[i]
#     return total_revenue # returns total revenue if none of the revenues is aggregate


def get_filing_currency(value: str, filing: 'edgar._filings.Filing') -> str | None:
    """
    Retrieves the currency for a specific value from an SEC filing's XBRL data,
    returning it in uppercase (e.g., 'USD' instead of 'usd').

    Args:
        value: The numeric value to search for in the facts (e.g., '89177000')
        filing: The SEC Filing object containing financial data

    Returns:
        The currency string in uppercase (e.g., 'USD') if found, otherwise None
    """
    try:
        facts = filing.obj().financials.xbrl_data.instance.query_facts(value=value)
        if not facts.empty and 'units' in facts.columns:
            currency = facts['units'].iat[0]
            return str(currency).upper() if currency else None
        return None
    except Exception as e:
        logger.info(f"Failed to extract currency: {str(e)}", exc_info=True)
        return None