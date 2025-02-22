import pandas as pd

import db_ops
import edgar_service
from config import  logger
import yfinance as yf

def request_fundamentals(ticker: str):
    try:
        ticker = yf.Ticker(ticker.replace('.','-'))
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

def update_fundamentals():
    # Update latest filings
    df_latest_filings = edgar_service.get_latest_filings()
    # Update latest filings
    if df_latest_filings is not None and not df_latest_filings.empty:
        db_ops.upsert_latest_filings(df_latest_filings)
    else:
        logger.warning('No latest filings to update')
    # Delete filings older than a month
    db_ops.delete_fillings_older_than_month()
    # Update fundamentals
    df_full_insert = pd.DataFrame(db_ops.get_latest_filings_for_full_insert())
    print(df_full_insert)