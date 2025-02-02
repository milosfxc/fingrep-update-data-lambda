import re
from collections import defaultdict
from datetime import datetime, timedelta
import config

headers = {"User-Agent": "milosfxc@gmail.com"}

pg_balance_sheet_columns = {'share_id': 'share_id',
                            'date': 'date',
                            'report_type': 'report_type',
                            'reportedCurrency': 'reportedCurrency',
                            'Total Assets': 'assets',
                            # Current Assets
                            'Cash And Cash Equivalents': 'cash_and_cash_equivalents',
                            'Other Short Term Investments': 'short_term_investments',
                            'Cash Cash Equivalents And Short Term Investments': 'cash_and_short_term_investments',
                            'Receivables': 'net_receivables',
                            'Inventory': 'inventory',
                            'Other Current Assets': 'other_current_assets',# must be calculated
                            'Current Assets': 'current_assets',
                            # Non-Current Assets
                            'Net PPE': 'property_plant_equipment_net',
                            'Goodwill': 'goodwill',
                            'Other Intangible Assets': 'intangible_assets',
                            'Investments And Advances': 'long_term_investments',
                            'Non Current Deferred Assets': 'non_current_deferred_assets',
                            'Other Non Current Assets': 'other_non_current_assets',# must be calculated
                            'Total Non Current Assets': 'non_current_assets',
                            # Current Liabilities
                            'Payables And Accrued Expenses': 'payables_and_expenses',
                            'Accounts Payable': 'account_payables',
                            'other_payables_and_expenses': 'other_payables_and_expenses',
                            'Current Debt And Capital Lease Obligation': 'short_term_debt',#
                            'Other Current Liabilities': 'other_current_liabilities',# must be calculated
                            'Current Liabilities': 'current_liabilities',
                            # Non-Current Liabilities
                            'Long Term Debt And Capital Lease Obligation': 'long_term_debt',
                            'Other Non Current Liabilities': 'other_non_current_liabilities',# must be calculated
                            'Total Non Current Liabilities Net Minority Interest': 'non_current_liabilities',
                            'Total Liabilities Net Minority Interest': 'liabilities',
                            # Equity
                            'Stockholders\' Equity': 'stockholders_equity',
                            'Capital Stock': 'capital_stock',
                            'Additional Paid in Capital': 'share_premium',
                            'Retained Earnings': 'retained_earnings',
                            'Treasury Stock': 'treasury_shares',
                            'Gains Losses Not Affecting Retained Earnings': 'accumulated_other_comprehensive_income_loss',#
                            'Minority Interest': 'minority_interest',
                            'Total Equity Gross Minority Interest': 'equity',
                            # Additional info
                            'Total Debt': 'debt',#
                            'Net Debt': 'net_debt',#
                            'Ordinary Shares Number': 'shares_outstanding'
                            }

pg_income_statement_columns = {'share_id': 'share_id',
                               'date': 'date',
                               'report_type': 'report_type',
                               'reportedCurrency': 'reportedCurrency',
                               'Total Revenue': 'revenue',
                               'Operating Revenue': 'operating_revenue',
                               'Cost Of Revenue': 'cost_of_revenue',
                               'Gross Profit': 'gross_profit',
                               'Operating Expense': 'operating_expenses',
                               'Selling General and Administrative': 'general_and_administrative_expenses',
                               'Depreciation Amortization Depletion': 'deprecation_and_amortization',
                               'Research & Development': 'research_and_development_expenses',
                               'Other Operating Expenses': 'other_operating_expenses',
                               'Operating Income': 'operating_income',
                               'EBITDA': 'ebitda',
                               'Reconciled Depreciation': 'reconciled_deprecation',
                               'EBIT': 'ebit',
                               'Net Non Operating Interest Income Expense': 'net_interest',
                               'Pretax Income': 'ebt',
                               'Tax Provision': 'income_tax',
                               'Net Income Common Stockholders': 'net_income',
                               'Basic EPS': 'eps',
                               'Diluted EPS': 'diluted_eps'
                               }

pg_cash_flow_columns = {'share_id': 'share_id',
                        'date': 'date',
                        'report_type': 'report_type',
                        'reportedCurrency': 'reportedCurrency',
                        # Operating CF
                        'Operating Cash Flow': 'operating_cash_flow',
                        'Net Income From Continuing Operations': 'operating_net_income',
                        'Operating Gains Losses': 'operating_gains_losses',
                        'Depreciation Amortization Depletion': 'operating_da',
                        'Deferred Tax': 'deferred_income_tax',
                        'Stock Based Compensation': 'share_based_compensation',
                        'Change In working capital': 'change_working_capital',# add column other operating activities
                        # Investing CF
                        'Investing Cash Flow': 'investing_cash_flow',
                        'Capital Expenditure Reported': 'capital_expenditure',
                        'Net PPE Purchase And Sale': 'investments_PPE',
                        'Net Business Purchase And Sale': 'acquisitions_net',
                        'Net Investment Purchase And Sale': 'purchases_of_investments', # add column other investing activities
                        # Financing CF
                        'Financing Cash Flow': 'financing_cash_flow',
                        'Net Issuance Payments of Debt': 'net_debt_issuance',
                        'Net Common Stock Issuance': 'net_common_shares_issued',
                        'Net Preferred Stock Issuance': 'net_preferred_shares_issued',
                        'Cash Dividends Paid': 'dividends_paid', # add column other financing activities
                        # Additional
                        'Free Cash Flow': 'free_cash_flow',
                        'End Cash Position': 'end_cash_balance',
                        'Beginning Cash Position': 'beginning_cash_balance',# add column change in cash
                        'Issuance Of Debt': 'debt_issued',
                        'Repayment Of Debt': 'debt_repayment'
                        }

pg_tables = {
    'balance_sheet': pg_balance_sheet_columns,
    'income_statement': pg_income_statement_columns,
    'cash_flow': pg_cash_flow_columns,
}

edgar_company_facts_positions_path = {
    'EntityCommonStockSharesOutstanding': ['facts', 'dei', 'EntityCommonStockSharesOutstanding', 'units', 'shares'],
    'CommonStockSharesOutstanding': ['facts', 'us-gaap', 'CommonStockSharesOutstanding', 'units', 'shares'],
}

allowed_share_type_ids = [1, 6, 10, 14, 17, 18, 19, 21, 24]


def remove_stock_suffix(input_string):
    if not input_string:
        return input_string
    pattern = r'(Class A Common Stock|Common Stock|Class A Ordinary Shares|Ordinary Shares|Ordinary Share)\s*(\(.+?\))?$'
    return re.sub(pattern, '', input_string).strip()

def get_formatted_utc_date():
    current_utc_date = datetime.utcnow() - timedelta(days=config.DAYS)
    return current_utc_date.strftime("%Y-%m-%d")


def check_nan_ohlc(df):
    tickers_with_nan = df.loc[df[['o', 'h', 'l', 'c']].isna().any(axis=1), 'T'].tolist()
    if tickers_with_nan:
        print("The following tickers had at least one NaN OHLC value on the date ", datetime.utcnow(), ":",
              tickers_with_nan)
        return df.dropna(subset=['o', 'h', 'l', 'c'])
    else:
        return df


def check_one_date(df):
    if not df['t'].nunique() == 1:
        print("#check_one_date#Not all tickers have the same date.")


def check_row_number(results_count, results_length):
    if results_count < 8000:
        print("resultsCount number is bellow 8000: ", results_count)
        print("Number of results: ", results_length)
    if results_count != results_length:
        print(f"resultCount length {results_count} doesn't match with the results length {results_length}")


magnified_columns_existing = ['open', 'high', 'low', 'close', 'volume', 'vwap']
magnified_columns_new = ['open', 'high', 'low', 'close', 'volume', 'vwap', 'rsi']

currency_values = defaultdict(dict)

# Non-monetary fields are used for currency conversion into dollars
non_monetary_columns = ['share_id', 'date', 'report_type', 'reportedCurrency', 'usd_exc']

# Mandatory columns
mandatory_columns = {
    "balance_sheet": ['assets', 'current_assets', 'non_current_assets', 'liabilities', 'equity'],
    "income_statement": ['revenue', 'gross_profit', 'ebitda'],
    'cash_flow': ['free_cash_flow', 'operating_cash_flow', 'investing_cash_flow', 'financing_cash_flow']
}