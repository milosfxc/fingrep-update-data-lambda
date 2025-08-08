import datetime
import re
from distutils.command.install import value

import numpy as np
import pandas as pd
import yfinance as yf

import config
import db_ops
import edgar_service_v2
import utils
from config import logger


def request_fundamentals(ticker: str):
    try:
        ticker = yf.Ticker(ticker.replace('.', '-'))
        financials = dict()
        currency = ticker.info.get('financialCurrency')
        if currency:
            currency = re.search(r"[A-Z]{3}", currency).group()
        else:
            currency = 'USD'
            country = ticker.info.get('country', '').strip()
            if country != 'United States':
                logger.warning(
                    f"Non-US ticker {ticker} (country: {country}) has no currency specified. \nDefaulting to USD. \nPlease verify currency manually.")


        financials['Currency'] = currency
        financials['BalanceSheet'] = ticker.balance_sheet
        financials['IncomeStatement'] = ticker.income_stmt
        financials['CashFlowStatement'] = ticker.cash_flow
        financials['BalanceSheetQ'] = ticker.quarterly_balance_sheet
        financials['IncomeStatementQ'] = ticker.quarterly_income_stmt
        financials['CashFlowStatementQ'] = ticker.quarterly_cash_flow

        for stmt in statements.keys():
            if financials[stmt] is None or financials[stmt].empty:
                financials.pop(stmt)

        return financials

    except Exception as e:
        logger.error(f"yfinance API request error for ticker {ticker}: {e}")
        return None


def get_company_fundamentals(ticker:str,share_id, report_date:str = None):
    yf_data = request_fundamentals(ticker)
    currency_id = db_ops.get_cached_foreign_keys()['currencies'][yf_data.pop('Currency')]
    if yf_data:
        stmt_dict = {}
        df_filings = edgar_service_v2.get_filings_by_company(ticker, config.report_start_date)
        for k, v in yf_data.items():
            stmt_positions = statements[k]
            v.index = v.index.str.lower()
            v = v.replace(np.nan, 0)
            dates_dict = v.to_dict()
            ans_date_dict = {}
            for date in v.columns:
                # Identification columns
                calendar_period = edgar_service_v2.get_calendar_period(date.strftime('%Y-%m-%d'), '10-Q' if k.endswith('Q') else '10-K')
                positions_dict = dates_dict[date]

                ans_positions_dict = {'share_id': share_id,
                                      'date': date,
                                      'currency_id': currency_id,
                                      'report_type': 'q' if k.endswith('Q') else 'a',
                                      'calendar_period_id': utils.report_periods.get(calendar_period),
                                      'filing_date': find_filing_date(df_filings,date)
                                      }
                find_filing_date(df_filings,date)
                for db_pos, calc_positions in stmt_positions.items():
                    sum_pos = 0
                    if calc_positions is None: continue
                    for pos in calc_positions:
                        pos = positions_dict.get(pos)
                        sum_pos +=  pos if pos else 0
                    ans_positions_dict[db_pos] = sum_pos
                # Check that statement has at least 70% of columns filled
                if sum(1 for v in ans_positions_dict.values() if v is None or v == 0)/len(ans_positions_dict) < 0.3:
                    ans_date_dict[date] = ans_positions_dict
            stmt_dict[k] = ans_date_dict
        return stmt_dict
    else:
        return None


def find_filing_date(df_filings: pd.DataFrame, yf_report_date: pd.Timestamp) -> datetime.datetime | None:

    if df_filings.empty:
        return None

    # Calculate absolute differences between reportDate and yf_report_date
    df_filings['time_diff'] = (df_filings['reportDate'] - yf_report_date).abs()

    # Find the minimum time difference
    min_diff = df_filings['time_diff'].min()

    # Check if the minimum difference is within 20 days
    if min_diff > pd.Timedelta(days=20):
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

statements = {
    'BalanceSheet': balance_sheet,
    'IncomeStatement': income_statement,
    'CashFlowStatement': cash_flow_statement,
    'BalanceSheetQ': balance_sheet,
    'IncomeStatementQ': income_statement,
    'CashFlowStatementQ': cash_flow_statement
}