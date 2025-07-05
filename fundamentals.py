import inspect
import sys
import traceback
from datetime import datetime
from typing import Optional, Dict
import os
import pickle
import edgar
import pandas as pd
from edgar import get_by_accession_number
from fastcore.imports import df_equal
from fmpsdk import balance_sheet_statement

import XBRLTagMapper
import db_ops
import edgar_service
import utils
from config import logger
import yfinance as yf

from utils import current_liabilities_list, non_current_liabilities_list


def request_fundamentals(ticker: str):
    try:
        ticker = yf.Ticker(ticker.replace('.', '-'))
        financials = dict()
        currency = ticker.info.get('financialCurrency')
        if currency is None:
            currency = 'USD'
            country = ticker.info.get('country', '').strip()
            if country != 'United States':
                logger.warning(
                    f"Non-US ticker {ticker} (country: {country}) has no currency specified. \nDefaulting to USD. \nPlease verify currency manually.")

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


def get_period_ending_by_accession_number(accession_number: str) -> str | None:
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


def get_filings_by_company(ticker: str) -> pd.DataFrame:
    company_filings = edgar.Company(ticker).get_filings(form=['10-K', '10-Q', '6-K', '20-F'],
                                                        filing_date='2020-01-01:').to_pandas()
    if not company_filings.empty:
        return company_filings[['accession_number', 'form', 'isXBRL']]
    else:
        return company_filings


def get_filing_details(accession_number: str, is_xbrl: int) -> Optional[Dict[str, pd.DataFrame]]:
    # Checks for xbrl tags
    if is_xbrl == 1:
        # filing data
        filing = edgar.get_by_accession_number(accession_number=accession_number)
        xbrl_data = filing.obj().financials.xbrl_data.instance
        # Schema check
        schema = 'us-gaap' if not xbrl_data.query_facts(schema='us-gaap').empty else 'ifrs-full'
        df_statements = {'balance_sheet': pd.DataFrame(), 'income_statement': pd.DataFrame(),
                         'cash_flow': pd.DataFrame()}
        xbrl_statements = XBRLTagMapper.xbrl_tags[schema]
        for stmt in xbrl_statements.keys():
            df_stmt = df_statements[stmt]
            for tag in xbrl_statements[stmt]:
                df_position = xbrl_data.query_facts(schema='us-gaap', concept=tag)[
                    ['value', 'concept', 'context_id', 'units', 'decimals', 'end_date']]
                if df_stmt.empty:
                    df_stmt = df_position
                elif not df_position.isna().all().all():
                    df_stmt = pd.concat([df_stmt, df_position])
            contexts_mapping = dict()
            range_context = None
            if not df_stmt.empty:
                context_ids = set(df_stmt['context_id'].unique())
                for context_id in context_ids:
                    context = xbrl_data.contexts.get(context_id)
                    if not context['dimensions']:  # This removes contexts for special dimensions such SEE region
                        if filing.period_of_report == context['instant']:
                            contexts_mapping[context_id] = context['instant']
                        elif filing.period_of_report == context['end_date']:
                            if range_context is None:
                                context['id'] = context_id
                                range_context = context
                            else:
                                temp_context_days = (
                                        datetime.strptime(context['end_date'], '%Y-%m-%d') - datetime.strptime(
                                    context['start_date'], '%Y-%m-%d')).days
                                range_context_days = (datetime.strptime(range_context['end_date'],
                                                                        '%Y-%m-%d') - datetime.strptime(
                                    range_context['start_date'], '%Y-%m-%d')).days
                                target = 365 if filing.form in {'10-K', '20-F'} else 90
                                nearest = min([temp_context_days, range_context_days], key=lambda x: abs(x - target))
                                if temp_context_days == nearest:
                                    context['id'] = context_id
                                    range_context = context
                if range_context:
                    contexts_mapping[range_context['id']] = range_context['end_date']

            print(df_stmt)
            df_stmt['context_id'] = df_stmt['context_id'].map(contexts_mapping)
            print(df_stmt)
            df_stmt.dropna(subset=['context_id'], inplace=True)
            df_statements[stmt] = df_stmt
        return df_statements
    elif is_xbrl == 0:
        return None
    else:
        logger.critical(f"Variable is_xbrl was not of type int, but it's {type(is_xbrl)}. Stopping the application.")
        sys.exit(1)


def get_filing_details_v2(accession_number:str, is_xbrl:int) -> Optional[Dict[str,pd.DataFrame]]:
    if  is_xbrl == 1:
        filing = edgar.get_by_accession_number(accession_number=accession_number)
        statements = {
            'BalanceSheet': filing.obj().financials.balance_sheet().to_dataframe(),
            'IncomeStatement': filing.obj().financials.income_statement().to_dataframe(),
            'CashFlowStatement': filing.obj().financials.cashflow_statement().to_dataframe()
        }
        # Acc standard, currency, period start
        ascps = get_period_start_and_currency_and_acc_standard(filing, statements['IncomeStatement'])
        # XBRL Mappings
        xbrl_map = dict()
        # Load data from pickle
        if os.path.exists('data/xbrl_map'):
            with open('data/xbrl_map', 'rb') as f:
                xbrl_map = pickle.load(f)
        for stmt in statements.keys():
            statements[stmt] = get_statement(filing, stmt, statements[stmt], ascps['acc_standard'], ascps['period_start'])
        return statements
    else:
        return None # todo yfinance

def get_statement(filing: edgar.Filing, stmt_name: str, df_stmt: pd.DataFrame, acc_standard: str, period_start: str) -> pd.DataFrame:
    stmt_tags = XBRLTagMapper.xbrl_tags[acc_standard][stmt_name]
    ans = {}
    for position, tags in stmt_tags.items():
        if tags is not None:
            ans[position] = get_position_value(filing, stmt_name, df_stmt, tags, period_start)
        else:
            ans[position] = None
    return pd.DataFrame({k: [v] for k, v in ans.items()})


def get_position_value(filing: edgar.Filing, stmt_name: str, df_stmt: pd.DataFrame, xbrl_tags: set, period_start: str) -> float | None:
    df_stmt['concept'] = df_stmt['concept'].str.replace(':', '_')
    df_stmt = df_stmt[df_stmt['concept'].isin(xbrl_tags)]
    period_end = filing.period_of_report
    values = pd.to_numeric(df_stmt[period_end], errors='coerce').dropna().tolist()
    # EXTRACTED XBRL INSTANCE DOCUMENT if statement has no values for position
    if not values:
        if stmt_name == 'BalanceSheet':
            df_tags = filing.xbrl().query().by_dimension(None).by_instant_date(period_end).to_dataframe('concept', 'numeric_value', 'statement_type')
        else:
            df_tags = filing.xbrl().query().by_dimension(None).by_date_range(period_start, period_end).to_dataframe('concept', 'numeric_value', 'statement_type')
            df_tags = df_tags[df_tags['statement_type'] != 'CashFlowStatement'] if stmt_name == 'IncomeStatement' else df_tags[df_tags['statement_type'] != 'IncomeStatement']
        df_tags = df_tags[df_tags['concept'].isin(xbrl_tags)]
        tag_values = pd.to_numeric(df_tags['numeric_value'], errors='coerce').dropna().tolist()

        if tag_values:
            values.append(max(tag_values, key=abs))
        if not values:
            return None
    max_value, sum_value = max(values), sum(values)
    pos_value = max_value if sum_value - max_value == max_value else sum_value
    return pos_value if pos_value != 0 else None


def get_period_start_and_currency_and_acc_standard(filing: edgar.Filing, df: pd.DataFrame) -> dict | None:
    values = pd.to_numeric(df[filing.period_of_report], errors='coerce').dropna()
    ans = {}
    # Accounting standard
    for concept in df['concept']:
        if concept.startswith('us-gaap'):
            ans['acc_standard'] = 'us-gaap'
            break
        elif concept.startswith('ifrs-full'):
            ans['acc_standard'] = 'ifrs-full'
            break
        else:
            ans['acc_standard'] = None
    # Currency and period start
    for value in values:
        if all(key in ans for key in ['currency', 'period_start']):
            return ans
        df_pos = filing.xbrl().query().by_statement_type('IncomeStatement').by_dimension(None).by_value(
            float(value)).to_dataframe('concept', 'period_start', 'period_end', 'unit_ref').drop_duplicates()
        if len(df_pos) == 1 and df_pos.loc[0, 'period_end'] == filing.period_of_report:
            ans['period_start'] = df_pos.loc[0, 'period_start']
            currency = get_currency(filing, df_pos.loc[0, 'unit_ref'])
            if currency:
                ans['currency'] = currency

    return None


def get_currency(filing: edgar.Filing, unit_ref: str) -> str | None:
    iso_cur = filing.xbrl().units[unit_ref]['measure'].split(':')
    if len(iso_cur) == 2 and 'iso4217' in iso_cur[0].lower() and len(iso_cur[1]) == 3:
        return iso_cur[1].upper()
    return None

def validate_balance_sheet(bs: dict[str,float]) -> pd.DataFrame:
    # Calc
    current_assets_total = sum(bs.get(position, 0) for position in utils.current_assets_list)
    non_current_assets_total = sum(bs.get(position, 0) for position in utils.non_current_assets_list)
    current_liabilities_total = sum(bs.get(position, 0) for position in utils.current_liabilities_list)
    non_current_liabilities_total = sum(bs.get(position, 0) for position in utils.non_current_liabilities_list)

    assets = bs.get('assets',0)
    current_assets = bs.get('current_assets',0)
    non_current_assets = bs.get('non_current_assets',0)
    liabilities = bs.get('liabilities',0)
    current_liabilities = bs.get('current_liabilities',0)
    non_current_liabilities = bs.get('non_current_liabilities',0)
    equity = max(bs.get('equity',0),bs.get('shareholders_equity',0))

    if not current_assets:
        current_assets = assets - non_current_assets if assets and non_current_assets else None
    if not non_current_assets:
        non_current_assets = assets - current_assets if assets and current_assets else None
    if not liabilities:
        liabilities = assets - equity if assets and equity else None
    if not current_liabilities:
        current_liabilities = liabilities - non_current_liabilities if liabilities and non_current_liabilities else None
    if not non_current_liabilities:
        non_current_liabilities = liabilities - current_liabilities if liabilities and non_current_liabilities else None
    # Calc other positions
    if current_assets and current_assets_total:
        bs['other_current_assets'] = current_assets - current_assets_total
    if non_current_assets and non_current_assets_total:
        bs['other_non_current_assets'] = non_current_assets - non_current_assets_total
    if current_liabilities and current_liabilities_total:
        bs['other_current_liabilities'] = current_liabilities - current_liabilities_total
    if non_current_liabilities and non_current_liabilities_total:
        bs['other_non_current_liabilities'] = non_current_liabilities = non_current_liabilities_total
    # Calc complex position
    # cash_and_short_term_investments
    cash_and_short_term_investments = bs.get('cash_and_short_term_investments',0)
    if not cash_and_short_term_investments:
        cash_and_cash_equivalents = bs.get('cash_and_cash_equivalents',0)
        short_term_investments = bs.get('short_term_investments',0)
        if cash_and_cash_equivalents or short_term_investments:
            bs['cash_and_short_term_investments'] = cash_and_cash_equivalents + short_term_investments
    # payables_and_expenses
    payables_and_expenses = bs.get('payables_and_expenses',0)
    if not payables_and_expenses:
        account_payables = bs.get('account_payables',0)
        accrued_liabilities_current = bs.get('accrued_liabilities_current', 0)
        if account_payables or accrued_liabilities_current:
            bs['payables_and_expenses'] = account_payables + accrued_liabilities_current

    return bs










