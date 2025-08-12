import datetime
import re
from typing import Optional

import numpy as np
import pandas as pd
import yfinance as yf
from torch import PRIVATE_OPS

import config
import db_ops
import edgar_service_v2
import utils
from utils import get_utc_date
from config import logger

# Shared variables
fundamentals_dict = dict()

def request_fundamentals(ticker: str) -> Optional[dict]:
    try:
        # Check if data is already requested
        if fundamentals := fundamentals_dict.get(ticker):
            return fundamentals
        # Fetch data
        yf_data = yf.Ticker(ticker.replace('.', '-'))
        fundamentals = dict()
        currency = yf_data.info.get('financialCurrency')
        if currency:
            currency = re.search(r"[A-Z]{3}", currency).group()
        else:
            currency = 'USD'
            country = yf_data.info.get('country', '').strip()
            if country != 'United States':
                logger.warning(
                    f"Non-US ticker {yf_data} (country: {country}) has no currency specified. \nDefaulting to USD. \nPlease verify currency manually.")


        fundamentals['Currency'] = currency
        fundamentals['BalanceSheet'] = yf_data.balance_sheet
        fundamentals['IncomeStatement'] = yf_data.income_stmt
        fundamentals['CashFlowStatement'] = yf_data.cash_flow
        fundamentals['BalanceSheetQ'] = yf_data.quarterly_balance_sheet
        fundamentals['IncomeStatementQ'] = yf_data.quarterly_income_stmt
        fundamentals['CashFlowStatementQ'] = yf_data.quarterly_cash_flow

        for stmt in statements_mapper.keys():
            if fundamentals[stmt] is None or fundamentals[stmt].empty:
                fundamentals.pop(stmt)
            else:
                fundamentals[stmt] = fundamentals[stmt]
        # Store requested data as copy to prevent modifications
        fundamentals_dict[ticker] = fundamentals.copy()

        return fundamentals

    except Exception as e:
        logger.error(f"yfinance API request error for ticker {ticker}: {e}")
        return None


def get_company_fundamentals(ticker:str, share_id, nearby_report_date:pd.Timestamp = None, requested_statement:str = None):
    yf_data = request_fundamentals(ticker)
    if yf_data:

        ans = {}
        df_filings = edgar_service_v2.get_filings_by_company(ticker, config.report_start_date)
        currency_id = db_ops.get_cached_foreign_keys()['currencies'][yf_data.pop('Currency')]
        # Iterate over quarterly and annual statements
        for stmt_name, df_stmt in yf_data.items():
            # Filter by statement
            if requested_statement and not stmt_name.startswith(requested_statement): continue
            # Fingrep statements
            stmt_positions_mapper = statements_mapper[stmt_name]
            # Converting yf position names to lower case
            df_stmt.index = df_stmt.index.str.lower()
            # Dates dict groups statements by report date
            dates_dict = df_stmt.to_dict()
            for date in df_stmt.columns:
                # Filter by report date
                if nearby_report_date and abs((date - nearby_report_date).days) > 31: continue
                # Identification columns
                calendar_period = edgar_service_v2.get_calendar_period(date.strftime('%Y-%m-%d'), '10-Q' if stmt_name.endswith('Q') else '10-K')
                yf_statement = dates_dict[date]
                stmt_dict = {'share_id': share_id,
                             'date': date,
                             'currency_id': currency_id,
                             'report_type': 'q' if stmt_name.endswith('Q') else 'a',
                             'calendar_period_id': utils.report_periods.get(calendar_period),
                             'filing_date': find_filing_date(df_filings,date, ticker)
                             }
                for db_pos, calc_positions in stmt_positions_mapper.items():
                    if calc_positions is None: continue
                    sum_pos = 0 * config.scale_factor
                    for pos in calc_positions:
                        pos = yf_statement.get(pos)
                        sum_pos +=  pos if pos else 0 # todo else 0 instead of None
                    stmt_dict[db_pos] = sum_pos
                # Check that statement has at least 70% of columns filled
                non_empty_columns = 0
                for key, value in stmt_dict.items():
                    if (key not in {'share_id', 'date', 'currency_id', 'report_type', 'calendar_period_id', 'filing_date'}
                            and value is not None and value != 0 and not (isinstance(value, float) and value != value)):
                        non_empty_columns += 1
                # Filter accepts statements with at least 4 non-identification columns
                if non_empty_columns > 3:
                    if date in ans:
                        ans[date][stmt_name] = stmt_dict
                    else:
                        ans[date] = {stmt_name:stmt_dict}
            # Insert data into database
            if ans:
                sorted_dict = dict(sorted(ans.items()))
                for date in sorted_dict:
                    stmts_dict = sorted_dict[date]
                    for key in ['IncomeStatement', 'CashFlowStatement', 'BalanceSheet', 'IncomeStatementQ', 'CashFlowStatementQ', 'BalanceSheetQ']:
                        if key in stmts_dict:
                            table_name = key[:-1] if key.endswith('Q') else key
                            if not db_ops.upsert_statement_v2(stmts_dict[key], utils.camel_to_snake(table_name),['share_id', 'report_type', 'date']):
                                return False
            else:
                logger.warning(f"{stmt_name} data for ticker {ticker} successfully requested via yf, but nothing to insert.")
                return False
        return True
    else:
        logger.warning(f"Couldn't request yf fundamentals for ticker {ticker}")
        return False



def find_filing_date(df_filings: pd.DataFrame, yf_report_date: pd.Timestamp, ticker: str) -> datetime.datetime | None:

    if getattr(df_filings, "empty", True):
        logger.warning(f"Couldn't request a list of all filings for ticker {ticker}")
        return None

    # Calculate absolute differences between reportDate and yf_report_date
    df_filings['time_diff'] = (df_filings['reportDate'] - yf_report_date).abs()

    # Find the minimum time difference
    min_diff = df_filings['time_diff'].min()

    # Check if the minimum difference is within 20 days
    if min_diff > pd.Timedelta(days=31):
        logger.debug(f"Returning None filing date for ticker {ticker} because min_diff > 31 days")
        return None

    # Get the row with the minimum time difference
    min_diff_row = df_filings.loc[df_filings['time_diff'].idxmin()]

    # Convert acceptanceDateTime to native Python datetime
    acceptance_dt = min_diff_row['acceptanceDateTime'].to_pydatetime()

    return acceptance_dt




balance_sheet = {'assets': ['total assets'],
                    'current_assets': ['current assets'],
                    'deposits': None,
                    'cash_and_short_term_investments': ['cash cash equivalents and short term investments'],
                    'cash_and_cash_equivalents': ['cash and cash equivalents'],
                    'short_term_investments': ['other short term investments'],
                    'net_receivables': ['receivables'],
                    'inventory': ['inventory'],
                    'other_current_assets': ['other current assets'],
                    'non_current_assets': ['total non current assets'],
                    'investments': None,
                    'real_estate': ['investment properties'],
                    'property_plant_equipment_net': ['net ppe'],
                    'goodwill': ['goodwill'],
                    'intangible_assets': ['other intangible assets'],
                    'long_term_investments': ['investments and advances'],
                    'non_current_deferred_assets': ['non current deferred assets'],
                    'other_non_current_assets': ['other non current assets'],
                    'liabilities': ['total liabilities net minority interest'],
                    'payables_and_expenses': ['payables and accrued expenses'],
                    'account_payables': ['accounts payable'],
                    'accrued_liabilities_current': ['current accrued expenses'],
                    'short_term_debt': ['current debt and capital lease obligation'],
                    'other_current_liabilities': ['other current liabilities'],
                    'current_liabilities': ['current liabilities'],
                    'long_term_debt': ['long term debt and capital lease obligation'],
                    'other_non_current_liabilities': ['other non current liabilities'],
                    'non_current_liabilities': ['total non current liabilities net minority interest'],
                    'equity': ['total equity gross minority interest'],
                    'minority_interest': ['minority interest'],
                    'shareholders_equity': ['stockholders equity'],
                    'capital_stock': ['capital stock'],
                    'share_premium': ['additional paid in capital'],
                    'retained_earnings': ['retained earnings'],
                    'treasury_shares': ['treasury stock'],
                    'accumulated_other_comprehensive_income_loss': ['gains losses not affecting retained earnings'],
                    'common_shares_outstanding': ['ordinary shares number'],
                    'preferred_shares_outstanding': None,
                    'debt': ['total debt'],
                    'net_debt': ['net debt']
                    }

income_statement = {'revenue': ['total revenue'],
                       'cost_of_revenue': ['cost of revenue'],
                       'gross_profit': ['gross profit'],
                       'operating_expenses': ['operating expense'],
                       'selling_general_and_administrative_expense': ['selling general and administration'],
                       'research_and_development_expenses': ['research and development'],
                       'other_operating_expenses': ['other operating expenses'],
                       'operating_income': ['operating income'],
                       'net_non_operating_income': ['other non operating income expenses'],
                       'interest_income': ['interest income'],
                       'interest_expense': ['interest expense'],
                       'net_interest': ['net interest income '],
                       'income_equity_method_investments': None,
                       'ebt': ['pretax income'],
                       'expenses': ['total expenses'],
                       'income_tax': ['tax provision'],
                       'net_income': ['net income common stockholders'],
                       'net_income_non_controlling_interests': ['minority interests'],
                       'net_income_including_non_controlling_interests': ['net income including noncontrolling interests'],
                       'ebit': ['ebit'],
                       'deprecation_and_amortization': ['reconciled depreciation'],
                       'ebitda': ['ebitda'],
                       'eps': ['basic eps'],
                       'diluted_eps': ['diluted eps'],
                       'avg_shares_basic': ['basic average shares'],
                       'avg_shares_diluted': ['diluted average shares'],
                       'non_interest_income': None, # revenue - net interest
                       'non_interest_expense': ['selling general and administration', 'occupancy and equipment', 'professional expense and contract services expense', 'other non interest expense'],
                       'provisions_credit_losses': None,
                       'net_premiums': None
                       }

cash_flow_statement = {'operating_cash_flow': ['operating cash flow'],
                          'operating_net_income': ['net income from continuing operations'],
                          'operating_da': ['depreciation amortization depletion'],
                          'deferred_income_tax': ['deferred tax'],
                          'share_based_compensation': ['stock based compensation'],
                          'change_working_capital': ['change in working capital'],
                          'other_operating_activities': None,
                          'investing_cash_flow': ['investing cash flow'],
                          'capital_expenditure': ['capital expenditure reported'],
                          'net_purchase_sale_ppe': ['net ppe purchase and sale'],
                          'net_business_acquisitions': ['net business purchase and sale'],
                          'net_purchase_sale_investments': ['net investment purchase and sale'],
                          'net_loan_lease_activity': None,
                          'other_investing_activities': None,
                          'financing_cash_flow': ['financing cash flow'],
                          'net_debt_issuance': ['net issuance payments of debt'],
                          'net_equity_issuance': ['net common stock issuance', 'net preferred stock issuance'],
                          'dividends_paid': ['cash dividends paid'],
                          'other_financing_activities': None,
                          'free_cash_flow': ['free cash flow'],
                          'start_cash_balance': ['beginning cash position'],
                          'change_in_cash': ['changes in cash'],
                          'end_cash_balance': ['end cash position']
                          }

statements_mapper = {
    'BalanceSheet': balance_sheet,
    'IncomeStatement': income_statement,
    'CashFlowStatement': cash_flow_statement,
    'BalanceSheetQ': balance_sheet,
    'IncomeStatementQ': income_statement,
    'CashFlowStatementQ': cash_flow_statement
}