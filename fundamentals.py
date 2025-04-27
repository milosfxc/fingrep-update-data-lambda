import pandas as pd
from edgar import get_by_accession_number

import db_ops
import edgar_service
import utils
from config import logger
import yfinance as yf


def request_fundamentals(ticker: str):
    try:
        ticker = yf.Ticker(ticker.replace('.', '-'))
        financials = dict()
        currency = ticker.info.get('financialCurrency')
        if currency is None:
            currency = 'USD'
            country = ticker.info.get('country', '').strip()
            if country != 'United States':
                logger.warning(f"Non-US ticker {ticker} (country: {country}) has no currency specified. \nDefaulting to USD. \nPlease verify currency manually.")

        financials['currency'] = currency
        financials['balance_sheet'] = ticker.balance_sheet
        financials['income_statement'] = ticker.income_stmt
        financials['cash_flow'] = ticker.cash_flow
        financials['balance_sheet_q'] = ticker.quarterly_balance_sheet
        financials['income_statement_q'] = ticker.quarterly_income_stmt
        financials['cash_flow_q'] = ticker.quarterly_cash_flow

        return financials

    except Exception as e:
        logger.error(f"yfinance API request error for ticker {ticker}: {e}")
        return None


def get_period_ending_by_accession_number(accession_number: str)-> str | None:
    """
    Retrieves the period ending date for a given accession number.

    Args:
        accession_number (str): The accession number of the filing.

    Returns:
        datetime or None: The period ending date if available, otherwise None.
    """
    try:
        # Retrieve the filing
        filing = get_by_accession_number(accession_number=accession_number)

        # Check if filing is None
        if filing is None:
            return None

        # Extract the period ending date
        period_ending = filing.period_of_report

        # Return the period ending date if it exists
        if period_ending:
            return period_ending
        else:
            return None
    except Exception as e:
        logger.error(f"Error retrieving period ending for accession number: {accession_number}: {e}", exc_info=True)
        return None

def calc_report_period_id(date: str, report_type: str):
    # Report period calculation
    row_date = pd.to_datetime(date)
    report_type = report_type
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
    # Return report period id
    return utils.report_periods[report_period]