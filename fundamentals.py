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


# def get_filing_details_v2(accession_number:str, is_xbrl:int) -> Optional[Dict[str,pd.DataFrame]]:
#     if  is_xbrl == 1:
#         filing = edgar.get_by_accession_number(accession_number=accession_number)
#         statements = {
#             'bs': filing.obj().financials.balance_sheet().to_dataframe(),
#             'in': filing.obj().financials.income_statement().to_dataframe(),
#             'cf': filing.obj().financials.cash_flow_statement().to_dataframe()
#         }
#
#         # XBRL Mappings
#         xbrl_map = dict()
#         # Load data from pickle
#         if os.path.exists('data/xbrl_map'):
#             with open('data/xbrl_map', 'rb') as f:
#                 xbrl_map = pickle.load(f)
#         for stmt in statements.keys():


def get_statement(stmt_name: str, df_stmt: pd.DataFrame, acc_standard: str, report_date: str) -> pd.DataFrame:
    stmt_tags = XBRLTagMapper.xbrl_tags[acc_standard][stmt_name]
    ans = {}
    for position, tags in stmt_tags.items():
        if tags is not None:
            ans[position] = get_position_value(df_stmt, tags, report_date)
    return pd.DataFrame({k: [v] for k, v in ans.items()})


def get_position_value(stmt_name: str, filing: edgar.Filing, df_stmt: pd.DataFrame, xbrl_tags: set[str],
                       start_date: str,
                       end_date: str) -> float | None:
    df_stmt = df_stmt[df_stmt['concept'].isin(xbrl_tags)]
    values = pd.to_numeric(df_stmt[end_date], errors='coerce').dropna().tolist()
    if not values:
        for tag in xbrl_tags:
            if stmt_name == 'BalanceSheet':
                df_tag = (filing.xbrl().query().by_dimension(None).by_concept(tag).
                          by_date_range(start_date, end_date).to_dataframe('numeric_value'))
            else:
                df_tag = (filing.xbrl().query().by_dimension(None).by_concept(tag).
                          by_instant_date(end_date).to_dataframe('numeric_value'))
            if not df_tag.empty:
                tag_values = df_tag['numeric_value'].dropna().tolist()
                tag_value = max(tag_values, key=abs)
                values.append(tag_value)
        return None
    max_value, sum_value = max(values), sum(values)
    pos_value = max_value if sum_value - max_value == max_value else sum_value
    return pos_value if pos_value != 0 else None


def get_period_start_and_currency(filing: edgar.Filing) -> dict | None:
    df = filing.xbrl().statements.income_statement().to_dataframe()
    values = pd.to_numeric(df[filing.period_of_report], errors='coerce').dropna()
    ans = {}
    for value in values:
        if all(key in ans for key in ['currency', 'period_end']):
            return ans
        df_pos = filing.xbrl().query().by_statement_type('IncomeStatement').by_dimension(None).by_value(
            float(value)).to_dataframe('concept', 'period_start', 'period_end', 'unit_ref').drop_duplicates()
        if len(df_pos) == 1 and df_pos.loc[0, 'period_end'] == filing.period_of_report:
            ans['period_end'] = df_pos.loc[0, 'period_start']
            currency = get_currency(filing, df_pos.loc[0, 'unit_ref'])
            if currency:
                ans['currency'] = currency

    return None


def get_currency(filing: edgar.Filing, unit_ref: str) -> str | None:
    iso_cur = filing.xbrl().units[unit_ref]['measure'].split(':')
    if len(iso_cur) == 2 and 'iso4217' in iso_cur[0].lower() and len(iso_cur[1]) == 3:
        return iso_cur[1].upper()
    return None
