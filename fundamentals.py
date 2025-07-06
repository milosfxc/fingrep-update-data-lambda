import inspect
import sys
import traceback
from datetime import datetime, timedelta
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


def get_filing_details(accession_number:str, is_xbrl:int) -> Optional[Dict[str,pd.DataFrame]]:
    if  is_xbrl == 1:
        filing = edgar.get_by_accession_number(accession_number=accession_number)
        statements = {
            'BalanceSheet': filing.obj().financials.balance_sheet().to_dataframe(),
            'IncomeStatement': filing.obj().financials.income_statement().to_dataframe(),
            'CashFlowStatement': filing.obj().financials.cashflow_statement().to_dataframe()
        }
        # Acc standard, currency, period start
        ascps = get_period_start_and_currency_and_acc_standard(filing, statements['IncomeStatement'])
        # Dataframes for XBRL query
        previous_end_date = (datetime.strptime(ascps['period_start'], '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
        print(previous_end_date)
        df_instant_prev_end = filing.xbrl().query().by_dimension(None).by_instant_date(previous_end_date).to_dataframe('concept', 'numeric_value', 'statement_type')
        df_instant_end = filing.xbrl().query().by_dimension(None).by_instant_date(filing.period_of_report).to_dataframe('concept', 'numeric_value', 'statement_type')
        df_period = filing.xbrl().query().by_dimension(None).by_date_range(ascps['period_start'], filing.period_of_report).to_dataframe('concept', 'numeric_value', 'statement_type')
        # XBRL Mappings
        xbrl_map = dict()
        # Load data from pickle
        if os.path.exists('data/xbrl_map'):
            with open('data/xbrl_map', 'rb') as f:
                xbrl_map = pickle.load(f)
        for stmt in statements.keys():
            statements[stmt] = get_statement(stmt, statements[stmt], ascps['acc_standard'], df_instant_end, df_period, filing.period_of_report)
        statements['BalanceSheet'] = validate_balance_sheet(statements['BalanceSheet'])
        statements['IncomeStatement'] = validate_income_statement(inc_stmt=statements['IncomeStatement'],cf_stmt=statements['CashFlowStatement'])
        statements['CashFlowStatement'] = validate_cashflow_statement(df_instant_prev_end,df_instant_end,df_period,statements['CashFlowStatement'], ascps['acc_standard'])
        return statements
    else:
        return None # todo yfinance

def get_statement(stmt_name: str, df_stmt: pd.DataFrame, acc_standard: str, df_instant: pd.DataFrame, df_period: pd.DataFrame, period_end: str) -> dict[str,float]:
    stmt_tags = XBRLTagMapper.xbrl_tags[acc_standard][stmt_name]
    df_period = df_period[df_period['statement_type'] != 'CashFlowStatement'] if stmt_name == 'IncomeStatement' else df_period[df_period['statement_type'] != 'IncomeStatement']
    df_tags = df_instant if stmt_name == 'BalanceSheet' else df_period
    # Replace _ with :
    df_stmt['concept'] = df_stmt['concept'].str.replace('_', ':')
    ans = {}
    for position, tags in stmt_tags.items():
        if tags is not None:
            ans[position] = get_position_value(df_stmt, df_tags,tags, period_end)
        else:
            ans[position] = None
    #return pd.DataFrame({k: [v] for k, v in ans.items()})
    return ans

def get_position_value(df_stmt: pd.DataFrame, df_tags: pd.DataFrame, xbrl_tags: set, period_end: str) -> float | None:
    df_stmt = df_stmt[df_stmt['concept'].isin(xbrl_tags)]
    values = pd.to_numeric(df_stmt[period_end], errors='coerce').dropna().tolist()
    # EXTRACTED XBRL INSTANCE DOCUMENT if statement has no values for position
    if not values:
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

def validate_balance_sheet(bs_stmt: dict[str,float]) -> dict[str,float] | None:
    # Calc
    current_assets_total = sum(bs_stmt.get(position) or 0 for position in utils.current_assets_list)
    non_current_assets_total = sum(bs_stmt.get(position) or 0 for position in utils.non_current_assets_list)
    current_liabilities_total = sum(bs_stmt.get(position) or 0 for position in utils.current_liabilities_list)
    non_current_liabilities_total = sum(bs_stmt.get(position) or 0 for position in utils.non_current_liabilities_list)
    print(type(bs_stmt))
    assets = bs_stmt.get('assets') or 0
    liab_and_equity = bs_stmt.get('liabilities_and_equity') or 0
    assets = max(assets, liab_and_equity)
    assets = assets if assets > 0 else None
    bs_stmt.pop('liabilities_and_equity')
    current_assets = bs_stmt.get('current_assets')
    non_current_assets = bs_stmt.get('non_current_assets')
    liabilities = bs_stmt.get('liabilities')
    current_liabilities = bs_stmt.get('current_liabilities')
    non_current_liabilities = bs_stmt.get('non_current_liabilities')
    equity = max(bs_stmt.get('equity') or 0, bs_stmt.get('shareholders_equity') or 0)
    equity = equity if equity > 0 else None
    # left side
    if assets:
        if not current_assets:
            current_assets = assets - non_current_assets if non_current_assets else None
        if not non_current_assets:
            non_current_assets = assets - current_assets if current_assets else None
        if not liabilities:
            liabilities = assets - equity if equity else None
    # right side
    if liabilities:
        if not current_liabilities:
            current_liabilities = liabilities - non_current_liabilities if non_current_liabilities else None
        if not non_current_liabilities:
            non_current_liabilities = liabilities - current_liabilities if current_liabilities else None
    # Update major balance sheet positions
    bs_stmt['current_assets'] = current_assets
    bs_stmt['non_current_assets'] = non_current_assets
    bs_stmt['liabilities'] = liabilities
    bs_stmt['current_liabilities'] = current_liabilities
    bs_stmt['non_current_liabilities'] = non_current_liabilities
    bs_stmt['equity'] = equity
    # Calc other positions
    if current_assets and current_assets_total:
        bs_stmt['other_current_assets'] = current_assets - current_assets_total
    if non_current_assets and non_current_assets_total:
        bs_stmt['other_non_current_assets'] = non_current_assets - non_current_assets_total
    if current_liabilities and current_liabilities_total:
        bs_stmt['other_current_liabilities'] = current_liabilities - current_liabilities_total
    if non_current_liabilities and non_current_liabilities_total:
        bs_stmt['other_non_current_liabilities'] = non_current_liabilities - non_current_liabilities_total
    # Calc complex position
    # cash_and_short_term_investments
    cash_and_short_term_investments = bs_stmt.get('cash_and_short_term_investments', 0)
    if not cash_and_short_term_investments:
        cash_and_cash_equivalents = bs_stmt.get('cash_and_cash_equivalents', 0)
        short_term_investments = bs_stmt.get('short_term_investments', 0)
        if cash_and_cash_equivalents or short_term_investments:
            bs_stmt['cash_and_short_term_investments'] = cash_and_cash_equivalents + short_term_investments
    # payables_and_expenses
    payables_and_expenses = bs_stmt.get('payables_and_expenses', 0)
    if not payables_and_expenses:
        account_payables = bs_stmt.get('account_payables', 0)
        accrued_liabilities_current = bs_stmt.get('accrued_liabilities_current', 0)
        if account_payables or accrued_liabilities_current:
            bs_stmt['payables_and_expenses'] = account_payables + accrued_liabilities_current

    return bs_stmt

def validate_income_statement(inc_stmt: dict[str,float], cf_stmt: dict[str, float]) -> dict[str,float] | None:
    revenue = inc_stmt.get('revenue')
    cost_of_revenue = abs(inc_stmt.get('cost_of_revenue'))
    gross_profit = inc_stmt.get('gross_profit')
    operating_income = inc_stmt.get('operating_income')
    operating_expenses = abs(inc_stmt.get('operating_expenses'))
    other_income = inc_stmt.get('other_income')
    ebt = inc_stmt.get('ebt')
    interest_expense = abs(inc_stmt.get('ebt')) # todo it could be positive
    dda = cf_stmt.get('operating_da')
    ebit = None
    ebitda = None
    if revenue:
        if gross_profit:
            cost_of_revenue = revenue - gross_profit
        elif cost_of_revenue:
            gross_profit = revenue - cost_of_revenue
    if operating_income:
        if gross_profit:
            operating_expenses = gross_profit - operating_income
        if ebt and not other_income:
            other_income = operating_income - ebt
        if not ebt and other_income:
            ebt = operating_income + other_income
    elif operating_expenses and gross_profit:
        operating_income = gross_profit - operating_expenses
    if ebt and interest_expense:
        ebit = ebt + interest_expense
    if ebit and dda:
        ebitda = ebit + dda

    inc_stmt['cost_of_revenue'] = cost_of_revenue
    inc_stmt['gross_profit'] = gross_profit
    inc_stmt['operating_income'] = operating_income
    inc_stmt['operating_expenses'] = operating_expenses
    inc_stmt['other_income'] = other_income
    inc_stmt['ebt'] = ebt
    inc_stmt['interest_expense'] = interest_expense
    inc_stmt['reconciled_deprecation'] = dda
    inc_stmt['ebit'] = ebit
    inc_stmt['ebitda'] = ebitda

    return inc_stmt

def validate_cashflow_statement(df_instant_start, df_instant_end, df_period, cf_stmt: dict[str,float], acc_standard: str) -> dict[str,float] | None:
    # CAPEX calc
    capital_expenditure = None
    df_ppe = df_period[df_period['concept'].isin(XBRLTagMapper.xbrl_tags[acc_standard]['CashFlowStatement']['purc_sale_ppe'])]
    capex_values = pd.to_numeric(df_ppe['numeric_value'], errors='coerce')
    capex_sum = capex_values[capex_values < 0].sum()
    capital_expenditure = capex_sum if capex_sum else None
    cf_stmt['capital_expenditure'] = capital_expenditure
    # Debt dataframe
    df_debt = df_period[df_period['concept'].isin(XBRLTagMapper.xbrl_tags[acc_standard]['CashFlowStatement']['iss_pay_debt'])]
    debt_values = pd.to_numeric(df_debt['numeric_value'], errors='coerce')
    # Debt Issued
    debt_issued = debt_values[debt_values > 0].sum()
    if debt_issued:
        cf_stmt['debt_issued'] = debt_issued
    # Debt Repayment
    debt_repayment = debt_values[debt_values < 0].sum()
    if debt_repayment:
        cf_stmt['debt_repayment'] = debt_repayment
    # Free Cash Flow
    free_cash_flow = None
    operating_cash_flow = cf_stmt.get('operating_cash_flow')
    if operating_cash_flow and capital_expenditure:
        free_cash_flow = operating_cash_flow + operating_cash_flow
        cf_stmt['free_cash_flow'] = free_cash_flow

    # Cash end
    df_cash_end = df_instant_end[df_instant_end['statement_type'] == 'CashFlowStatement']
    df_cash_end = df_cash_end[df_cash_end['concept'].isin(XBRLTagMapper.xbrl_tags[acc_standard]['CashFlowStatement']['end_cash_balance'])]
    end_cash_balance = df_cash_end['numeric_value'].max()
    if end_cash_balance:
        cf_stmt['end_cash_balance'] = end_cash_balance
    # Cash start
    start_cash_balance = None
    net_change = cf_stmt.get('inc_dec_cash')
    if net_change and end_cash_balance:
        start_cash_balance = end_cash_balance - net_change
    else:
        df_cash_start = df_instant_start[df_instant_start['statement_type'] == 'CashFlowStatement']
        df_cash_start = df_cash_start[df_cash_start['concept'].isin(XBRLTagMapper.xbrl_tags[acc_standard]['CashFlowStatement']['start_cash_balance'])]
        start_cash_balance = df_cash_start['numeric_value'].max()
    if start_cash_balance:
        cf_stmt['start_cash_balance'] = start_cash_balance

    return cf_stmt























