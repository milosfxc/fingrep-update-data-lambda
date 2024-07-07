import os
import pandas as pd
import utils
from db_ops import upsert_financials
import logging
import fmpsdk as fmp
import datetime
# Pandas configuration
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 400)


def get_fundamentals(cik: str, share_id: int, currency_id: int, ticker: str, period: str, date: datetime = None):
    try:
        fmp_api_key = os.getenv('FMP_API_KEY')
        data_dict = {
            'balance_sheet': fmp.balance_sheet_statement(apikey=fmp_api_key, symbol=ticker, period=period),
            'income_statement': fmp.income_statement(apikey=fmp_api_key, symbol=ticker, period=period),
            'cash_flow': fmp.cash_flow_statement(apikey=fmp_api_key, symbol=ticker, period=period)
        }
        for table_name, data in data_dict.items():
            if 'Error Message' in data:
                logging.error(f"Error message for {ticker}: {data['Error Message']}")
            else:
                df = pd.DataFrame(data)
                if date is not None:
                    date_str = date.strftime('%Y-%m-%d')
                    df = df.query('date > @date_str')
                if 'cik' not in df.columns or df.iloc[0]['cik'] != cik.zfill(10):
                    logging.error(f"CIK number not present or mismatch for {ticker}")
                    return None
                df.loc[:, ['period', 'share_id', 'currency_id']] = 'Annual', share_id, currency_id
                df = df[utils.pg_tables.get(table_name).keys()]
                df = df.rename(columns=utils.pg_tables.get(table_name))
                upsert_financials(df, table_name)
    except Exception as e:
        logging.error(f"API request error: {e}")
