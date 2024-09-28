import datetime
import time
from http.client import responses
import re

import pandas as pd
import requests
import logging

from bs4 import BeautifulSoup
from fmpsdk import cik_list

import utils

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
    cik = cik.zfill(10)
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
                logger.critical(f"Error getting company facts for CIK {cik}: {e}")
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
        try:
            response = response[position]
        except KeyError:
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