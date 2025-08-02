import re
from datetime import datetime, timedelta, timezone
from typing import Union

import config

headers = {"User-Agent": "milosfxc@gmail.com"}

pg_balance_sheet_columns = {'share_id': 'share_id',
                            'date': 'date',
                            'report_type': 'report_type',
                            'filing_date': 'filing_date',
                            'report_period_id': 'report_period_id',
                            'currency_id': 'currency_id',
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
                               'currency_id': 'currency_id',
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
                               'Basic Average Shares': 'avg_shares_outstanding',
                               # Bank
                               'Interest Income': 'interest_income',
                               'Interest Expense': 'interest_expense',
                               'Credit Losses Provision': 'credit_losses_provision',
                               'Non Interest Expense': 'non_interest_expense'
                               }

pg_cash_flow_columns = {'share_id': 'share_id',
                        'date': 'date',
                        'report_type': 'report_type',
                        'filing_date': 'filing_date',
                        'report_period_id': 'report_period_id',
                        'currency_id': 'currency_id',
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
balance_sheet_gaap = ['us-gaap:AssetsCurrent', 'us-gaap:CashAndCashEquivalentsAtCarryingValue', 'us-gaap:Cash',
                           'us-gaap:RestrictedCashAndCashEquivalentsAtCarryingValue', 'us-gaap:RestrictedCash',
                           'us-gaap:ShortTermInvestments', 'us-gaap:AvailableForSaleSecuritiesDebtSecuritiesCurrent', 'us-gaap:AccountsReceivableNetCurrent', 'us-gaap:InventoryNet']
balance_sheet_ifrs = ['ifrs-full:CurrentAssets', 'ifrs-full:CashAndCashEquivalents', 'ifrs-full:Cash', 'ifrs-full:TradeAndOtherCurrentReceivables', 'ifrs-full:Inventories']

income_statement = ['us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax', 'us-gaap:CostOfGoodsAndServicesSold', 'us-gaap:GrossProfit', 'us-gaap:EarningsPerShareBasic', 'us-gaap:EarningsPerShareDiluted']
xbrl_tags = {
    'us-gaap': {'balance_sheet': balance_sheet_gaap, 'income_statement': income_statement}, # 'cash_flow': None},
    'ifrs-full': {'balance_sheet': balance_sheet_ifrs}#, 'income_statement': None, 'cash_flow': None}
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

def get_utc_date(days: int = 0, as_str: bool = True) -> Union[str, datetime]:
    current_utc_date = datetime.now(timezone.utc) - timedelta(days=days)
    return current_utc_date.strftime("%Y-%m-%d") if as_str else current_utc_date


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

# Non-monetary columns
non_monetary_columns = ['share_id', 'date', 'report_type', 'filing_date', 'report_period_id', 'currency_id']

# Mandatory columns
mandatory_columns = {
    "balance_sheet": ['assets', 'current_assets', 'non_current_assets', 'liabilities', 'equity'],
    "income_statement": ['revenue'],
    'cash_flow': ['operating_cash_flow', 'investing_cash_flow', 'financing_cash_flow']
}

report_periods = {'2020': 1, '2020Q1': 2, '2020Q2': 3, '2020Q3': 4, '2020Q4': 5, '2021': 6, '2021Q1': 7, '2021Q2': 8,
                  '2021Q3': 9, '2021Q4': 10, '2022': 11, '2022Q1': 12, '2022Q2': 13, '2022Q3': 14, '2022Q4': 15, '2023': 16,
                  '2023Q1': 17, '2023Q2': 18, '2023Q3': 19, '2023Q4': 20, '2024': 21, '2024Q1': 22, '2024Q2': 23, '2024Q3': 24,
                  '2024Q4': 25, '2025': 26, '2025Q1': 27, '2025Q2': 28, '2025Q3': 29, '2025Q4': 30, '2026': 31, '2026Q1': 32,
                  '2026Q2': 33, '2026Q3': 34, '2026Q4': 35, '2027': 36, '2027Q1': 37, '2027Q2': 38, '2027Q3': 39, '2027Q4': 40,
                  '2028': 41, '2028Q1': 42, '2028Q2': 43, '2028Q3': 44, '2028Q4': 45, '2029': 46, '2029Q1': 47, '2029Q2': 48,
                  '2029Q3': 49, '2029Q4': 50, '2030': 51, '2030Q1': 52, '2030Q2': 53, '2030Q3': 54, '2030Q4': 55}

report_types_id = {'a': 1, 'q': 2}
form_report_type = {'10-K': 'a', '10-Q': 'q', '20-F': 'a', '6-K': 'q'}

current_assets_list = ['cash_and_short_term_investments', 'net_receivables', 'inventory']
non_current_assets_list = ['property_plant_equipment_net', 'goodwill', 'intangible_assets', 'long_term_investments', 'non_current_deferred_assets']
current_liabilities_list = ['payables_and_expenses', 'short_term_debt']
non_current_liabilities_list = ['long_term_debt']

forms = {
    'annual': {
        '10-K', '10-K/A',  # U.S. domestic annual reports
        '20-F', '20-F/A',  # Foreign private issuer annual reports (non-Canadian)
        '40-F', '40-F/A',  # Canadian issuer annual reports (MJDS filings)
        '10-KT', '20-FT'   # Transition reports (rare, but annual-like)
    },
    'quarterly': {
        '10-Q', '10-Q/A',  # U.S. domestic quarterly reports
        '6-K'              # Foreign issuer current reports (often quarterly updates)
    },
    'other': {
        '8-K',             # Current events (e.g., mergers, leadership changes)
        'DEF 14A',         # Proxy statements
        'S-1', 'F-1'       # Registration statements (IPOs)
    }
}