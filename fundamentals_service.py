import datetime
import edgar
import edgar_service_v2
import yahoo_service
from edgar_service_v2 import get_filings_by_company, get_q4_statements, get_filing_details
from utils import camel_to_snake,forms,get_utc_date
from config import logger
import db_ops


def get_company_fundamentals(ticker: str, share_id: int, cutoff_date:str):
    df_filings = get_filings_by_company(ticker, cutoff_date)
    if df_filings.empty:
        logger.warning(f"Filings for ticker {ticker} where missing or df_filings doesn't have required all required columns. Columns list: {df_filings.columns}")
        # Fetch with yfinance if form 20-F because Q reports aren't available or if company published only 10-K via Edgar such as Oracle (based in Ireland)
    elif df_filings['form'].isin(['20-F']).any() or (not df_filings['form'].isin(['10-Q']).any() and df_filings['form'].isin(['10-K']).sum() <= 1):
        yahoo_service.get_company_fundamentals(ticker,share_id)
    else:
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
                        logger.warning(f"Error preparing for insert Q4 statements for new ticker {ticker}. \n{e.with_traceback()}")
            else:
                # Yahoo alternative
                yahoo_service.get_company_fundamentals(ticker=ticker, share_id=share_id, nearby_report_date=row['reportDate'])


def update_fundamentals():
    # Daily update
    df_filings = edgar_service_v2.get_latest_filings(get_utc_date(days=5,as_str=True))
    ticker_and_share_id_by_cik = db_ops.get_foreign_keys()['ticker_and_share_id_by_cik']
    processed_filings = db_ops.get_accession_numbers()
    filings = []
    for index, row in df_filings.iterrows():
        if row['cik'] in ticker_and_share_id_by_cik and row['accession_number'] not in processed_filings: # todo add check edgar_api.get_ticker_by_cik if cik changes than it must be matched by ticker
            row_dict = row.to_dict()
            row_dict.update(ticker_and_share_id_by_cik[row['cik']])
            row_dict['inserted'] = update_company_fundamentals(row_dict['ticker'],row_dict['share_id'],row_dict['form'],row_dict['accession_number'])
            row_dict['attempt_date'] = None
            filings.append(row_dict)
    # Update latest filings
    db_ops.upsert_latest_filings(filings)
    # Get filings older than 5 days
    df_latest_filings = db_ops.get_latest_filings_for_insert()
    latest_filings = list()
    for index, row in df_latest_filings.iterrows():
        row_dict = row.to_dict()
        row_dict['inserted'] = yahoo_service.get_company_fundamentals(row['ticker'],row['share_id'])
        row_dict['attempt_date'] = datetime.datetime.now()
        latest_filings.append(row_dict)
    # Update latest filings retries
    db_ops.upsert_latest_filings(latest_filings)


def update_company_fundamentals(ticker: str, share_id: int, form: str, accession_number: str) -> bool:
    try:
        if form in ['10-K','10-Q']:
            # Get filing for report period
            filing = edgar.get_by_accession_number(accession_number)
            report_date = datetime.datetime.strptime(filing.period_of_report, '%Y-%m-%d')
            # Get statements for insert
            statements = get_filing_details(accession_number,1,share_id, filing)

            if statements:
                if len(statements) < 3:
                    logger.warning(f"Filing with accession number {accession_number} returned less than 3 statements: {statements.keys()}")
                for k in ['IncomeStatement', 'CashFlowStatement', 'BalanceSheet']: # Must be this order for the Ratios trigger function
                    if k in statements:
                        if not db_ops.upsert_statement_v2(statements[k], camel_to_snake(k), ['share_id', 'report_type', 'date']):
                            logger.warning(f"Error inserting {k} for filing with {accession_number}")
                            return False
                # Q4 Statements calculation
                if form == '10-K':
                    try:
                        q_statements = get_q4_statements(statements, share_id,report_date)
                        if q_statements:
                            if len(statements) < 3:
                                logger.warning(f"Calculation for Q4 statements based on 10-K filing with accession number {accession_number} returned less than 3 statements: {statements.keys()}")
                            for k, v in q_statements.items():
                                if not db_ops.upsert_statement_v2(v, camel_to_snake(k), ['share_id', 'report_type', 'date']):
                                    logger.warning(f"Error inserting Q4 {k} for filing with {accession_number}")
                                    return False
                        else:
                            logger.warning(f"Calculation for Q4 statements based on 10-K filing with accession number {accession_number} failed.")
                            return False
                    except Exception as e:
                        logger.warning(f"Error preparing for insert Q4 statements for ticker {ticker}. \n{e.with_traceback()}")
                        return False
            else:
                if filing.xbrl():
                    logger.warning(f"Couldn't obtain statements for:\n Ticker: {ticker} \nAccession Number: {accession_number}\n Share ID: {share_id}")
                return False
        else:
            return False
        return True
    except Exception as e:
        logger.warning(f" - Error during updating fundamentals with accession_number {accession_number} for ticker {ticker}.")
        return False