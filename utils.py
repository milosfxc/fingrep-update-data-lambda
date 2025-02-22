import re
from collections import defaultdict
from datetime import datetime, timedelta
import config

headers = {"User-Agent": "milosfxc@gmail.com"}

pg_balance_sheet_columns = {'share_id': 'share_id',
                            'date': 'date',
                            'report_type': 'report_type',
                            'filing_date': 'filing_date',
                            'report_period_id': 'report_period_id',
                            'currency': 'currency',
                            'total assets': 'assets',
                            # Current assets
                            'cash and cash equivalents': 'cash_and_cash_equivalents',
                            'other short term investments': 'short_term_investments',
                            'cash cash equivalents and short term investments': 'cash_and_short_term_investments',
                            'receivables': 'net_receivables',
                            'inventory': 'inventory',
                            'other current assets': 'other_current_assets',# must be calculated
                            'current assets': 'current_assets',
                            # Non-current assets
                            'net ppe': 'property_plant_equipment_net',
                            'goodwill': 'goodwill',
                            'other intangible assets': 'intangible_assets',
                            'investments and advances': 'long_term_investments',
                            'non current deferred assets': 'non_current_deferred_assets',
                            'other non current assets': 'other_non_current_assets',# must be calculated
                            'total non current assets': 'non_current_assets',
                            # Current liabilities
                            'payables and accrued expenses': 'payables_and_expenses',
                            'accounts payable': 'account_payables',
                            'other_payables_and_expenses': 'other_payables_and_expenses',
                            'current debt and capital lease obligation': 'short_term_debt',#
                            'other current liabilities': 'other_current_liabilities',# must be calculated
                            'current liabilities': 'current_liabilities',
                            # Non-current liabilities
                            'long term debt and capital lease obligation': 'long_term_debt',
                            'other non current liabilities': 'other_non_current_liabilities',# must be calculated
                            'total non current liabilities net minority interest': 'non_current_liabilities',
                            'total liabilities net minority interest': 'liabilities',
                            # Equity
                            'stockholders equity': 'shareholders_equity',
                            'capital stock': 'capital_stock',
                            'additional paid in capital': 'share_premium',
                            'retained earnings': 'retained_earnings',
                            'treasury stock': 'treasury_shares',
                            'gains losses not affecting retained earnings': 'accumulated_other_comprehensive_income_loss',#
                            'minority interest': 'minority_interest',
                            'total equity gross minority interest': 'equity',
                            # Additional info
                            'total debt': 'debt',#
                            'net debt': 'net_debt',#
                            'ordinary shares number': 'shares_outstanding',
                            'common stock equity': 'common_stock_equity'
                            }

pg_income_statement_columns = {'share_id': 'share_id',
                               'date': 'date',
                               'report_type': 'report_type',
                               'filing_date': 'filing_date',
                               'report_period_id': 'report_period_id',
                               'currency': 'currency',
                               'total revenue': 'revenue',
                               'operating revenue': 'operating_revenue',
                               'cost of revenue': 'cost_of_revenue',
                               'gross profit': 'gross_profit',
                               'operating expense': 'operating_expenses',
                               'selling general and administration': 'general_and_administrative_expenses',
                               'depreciation amortization depletion income statement': 'depreciation_amortization_depletion',
                               'research and development': 'research_and_development_expenses',
                               'other operating expenses': 'other_operating_expenses',
                               'operating income': 'operating_income',
                               'ebitda': 'ebitda',
                               'reconciled depreciation': 'reconciled_deprecation',
                               'ebit': 'ebit',
                               'net non operating interest income expense': 'net_interest',
                               'pretax income': 'ebt',
                               'tax provision': 'income_tax',
                               'net income common stockholders': 'net_income',
                               'basic eps': 'eps',
                               'diluted eps': 'diluted_eps',
                               'Basic Average Shares': 'avg_shares_outstanding'
                               }

pg_cash_flow_columns = {'share_id': 'share_id',
                        'date': 'date',
                        'report_type': 'report_type',
                        'filing_date': 'filing_date',
                        'report_period_id': 'report_period_id',
                        'currency': 'currency',
                        # Operating cf
                        'operating cash flow': 'operating_cash_flow',
                        'net income from continuing operations': 'operating_net_income',
                        'operating gains losses': 'operating_gains_losses',
                        'depreciation amortization depletion': 'operating_da',
                        'deferred tax': 'deferred_income_tax',
                        'stock based compensation': 'share_based_compensation',
                        'change in working capital': 'change_working_capital',# add column other operating activities
                        # Investing cf
                        'investing cash flow': 'investing_cash_flow',
                        'capital expenditure reported': 'capital_expenditure',
                        'net ppe purchase and sale': 'investments_ppe',
                        'net business purchase and sale': 'acquisitions_net',
                        'net investment purchase and sale': 'purchases_of_investments', # add column other investing activities
                        # Financing cf
                        'financing cash flow': 'financing_cash_flow',
                        'net issuance payments of debt': 'net_debt_issuance',
                        'net common stock issuance': 'net_common_shares_issued',
                        'net preferred stock issuance': 'net_preferred_shares_issued',
                        'cash dividends paid': 'dividends_paid', # add column other financing activities
                        # Additional
                        'free cash flow': 'free_cash_flow',
                        'end cash position': 'end_cash_balance',
                        'beginning cash position': 'beginning_cash_balance',# add column change in cash
                        'issuance of debt': 'debt_issued',
                        'repayment of debt': 'debt_repayment'
                        }

pg_tables = {
    'balance_sheet': pg_balance_sheet_columns,
    'income_statement': pg_income_statement_columns,
    'cash_flow': pg_cash_flow_columns,
}


edgar_company_facts_positions_path = {
    'EntityCommonStockSharesOutstanding': ['facts', 'dei', 'EntityCommonStockSharesOutstanding', 'units', 'shares'],
    'CommonStockSharesOutstanding': ['facts', 'us-gaap', 'CommonStockSharesOutstanding', 'units', 'shares'],
    'Assets': ['facts', 'us-gaap', 'Assets', 'units', 'USD'],

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

# Magnification
magnified_columns_existing = ['open', 'high', 'low', 'close', 'volume', 'vwap']
magnified_columns_new = ['open', 'high', 'low', 'close', 'volume', 'vwap', 'rsi']

# Non-monetary fields are used for currency conversion into dollars
non_monetary_columns = ['share_id', 'date', 'report_type', 'currency', 'usd_exc']

# Mandatory columns
mandatory_columns = {
    "balance_sheet": ['assets', 'current_assets', 'non_current_assets', 'liabilities', 'equity'],
    "income_statement": ['revenue','ebitda'],
    'cash_flow': ['operating_cash_flow', 'investing_cash_flow', 'financing_cash_flow']
}

report_periods = {'2020': 1, '2020q1': 2, '2020q2': 3, '2020q3': 4, '2020q4': 5, '2021': 6, '2021q1': 7, '2021q2': 8,
                  '2021q3': 9, '2021q4': 10, '2022': 11, '2022q1': 12, '2022q2': 13, '2022q3': 14, '2022q4': 15, '2023': 16,
                  '2023q1': 17, '2023q2': 18, '2023q3': 19, '2023q4': 20, '2024': 21, '2024q1': 22, '2024q2': 23, '2024q3': 24,
                  '2024q4': 25, '2025': 26, '2025q1': 27, '2025q2': 28, '2025q3': 29, '2025q4': 30, '2026': 31, '2026q1': 32,
                  '2026q2': 33, '2026q3': 34, '2026q4': 35, '2027': 36, '2027q1': 37, '2027q2': 38, '2027q3': 39, '2027q4': 40,
                  '2028': 41, '2028q1': 42, '2028q2': 43, '2028q3': 44, '2028q4': 45, '2029': 46, '2029q1': 47, '2029q2': 48,
                  '2029q3': 49, '2029q4': 50, '2030': 51, '2030q1': 52, '2030q2': 53, '2030q3': 54, '2030q4': 55}

report_types = {'a': 1, 'q': 2}
form_report_type_id = {'10-K': 1, '10-Q': 2, '20-F': 1, '6-K': 2}