import yahoo_service
from edgar_service_v2 import get_filings_by_company, get_q4_statements, get_filing_details
from utils import camel_to_snake,forms
from config import logger
import db_ops

def get_company_fundamentals(ticker: str, share_id: int, cutoff_date:str):
    df_filings = get_filings_by_company(ticker, cutoff_date)
    if df_filings.empty:
        logger.warning(f"Filings for ticker {ticker} where missing or df_filings doesn't have required all required columns. Columns list: {df_filings.columns}")
        return
    # Fetch filings
    for index, row in df_filings.iterrows():
        statements = get_filing_details(row['accession_number'], is_xbrl=row['isXBRL'], share_id=share_id)
        if statements:
            for k in ['IncomeStatement', 'CashFlowStatement', 'BalanceSheet']: # Must be this order for the Ratios trigger function
                if k in statements:
                    db_ops.upsert_statement_v2(statements[k], camel_to_snake(k), ['share_id', 'report_type', 'date'])

            # Q4 Statements calculation
            if row['form'] in forms['annual']:
                try:
                    q_statements = get_q4_statements(statements, share_id, row['reportDate'].date())
                    for k, v in q_statements.items():
                        db_ops.upsert_statement_v2(v, camel_to_snake(k), ['share_id', 'report_type', 'date'])
                except Exception as e:
                    logger.warning(f"Error for Q4 statements insertion for ticker {ticker}. Filing date of annual statement {e.with_traceback()}")
        else:
            print('Yahoo...')
            # Yahoo alternative
            if statements is None:
                yahoo_service.get_company_fundamentals(ticker, share_id, row['reportDate'])