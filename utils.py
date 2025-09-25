import math
import re
from datetime import datetime, timedelta, timezone
from typing import Union


headers = {"User-Agent": "milosfxc@gmail.com"}


edgar_company_facts_positions_path = {
    'EntityCommonStockSharesOutstanding': ['facts', 'dei', 'EntityCommonStockSharesOutstanding', 'units', 'shares'],
    'CommonStockSharesOutstanding': ['facts', 'us-gaap', 'CommonStockSharesOutstanding', 'units', 'shares'],
    'Assets': ['facts', 'us-gaap', 'Assets', 'units', 'USD'],
}

allowed_share_type_ids = [1, 10, 14, 17, 18, 19, 21, 24]# todo bring back 6 (ETF)


def remove_stock_suffix(input_string):
    if not input_string:
        return input_string
    pattern = r'(Class A Common Stock|Common Stock|Class A Ordinary Shares|Ordinary Shares|Ordinary Share)\s*(\(.+?\))?$'
    return re.sub(pattern, '', input_string).strip()

def get_utc_date(days: int = 0, as_str: bool = True) -> Union[str, datetime]:
    current_utc_date = datetime.now(timezone.utc) - timedelta(days=days)
    return current_utc_date.strftime("%Y-%m-%d") if as_str else current_utc_date


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
        '10-K',            # U.S. domestic annual reports
        '20-F',            # Foreign private issuer annual reports (non-Canadian)
        '40-F',            # Canadian issuer annual reports (MJDS filings)
    },
    'quarterly': {
        '10-Q',     # U.S. domestic quarterly reports
        '6-K'       # Foreign issuer current reports (often quarterly updates)
    },
    'other': {
        '8-K',      # Current events (e.g., mergers, leadership changes)
        'DEF 14A',  # Proxy statements
        'S-1', 'F-1'# Registration statements (IPOs)
    }
}

def camel_to_snake(name) -> str:
    # Insert underscores before capital letters, lowercase everything
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)
    return name.lower()


def replace_dict_nan_with_none(obj):
    if isinstance(obj, dict):
        return {k: replace_dict_nan_with_none(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_dict_nan_with_none(v) for v in obj]
    elif isinstance(obj, float) and math.isnan(obj):
        return None
    else:
        return obj

def or_zero(val):
    return val if val is not None else 0