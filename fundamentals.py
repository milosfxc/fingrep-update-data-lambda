import datetime

from config import  logger
import yfinance as yf

def get_fundamentals(ticker: str):
    try:
        ticker = yf.Ticker(ticker)
        financials = dict()
        financials['reportedCurrency'] = ticker.info['financialCurrency']
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