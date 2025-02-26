import pandas as pd
from edgar import get_by_accession_number

import db_ops
import edgar_service
from config import logger
import yfinance as yf


def request_fundamentals(ticker: str):
    try:
        ticker = yf.Ticker(ticker.replace('.', '-'))
        financials = dict()
        financials['currency'] = ticker.info.get('financialCurrency')
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