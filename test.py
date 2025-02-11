import time
from datetime import datetime, timezone, timedelta, date


import config
import edgar
import fingrep_service
import forex
import utils
import fundamentals
import pandas as pd
from config import logger
# Set pandas to display all rows and columns
pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', None)  # To allow the console to use the full width


# def get_and_insert_fundamentals(share_id: int, ticker: str, period_ending: datetime = None):
#     fundamentals_dict = fundamentals.get_fundamentals(ticker=ticker)
#     if fundamentals_dict is None:
#         time.sleep(3)
#         fundamentals_dict = fundamentals.get_fundamentals(ticker=ticker)
#         if fundamentals_dict is None:
#             logger.error(f"get_and_insert_fundamentals: Skipping fundamentals insertion for {ticker}")
#             return
#     for statement in ['balance_sheet']: #['income_statement', 'cash_flow', 'balance_sheet', 'income_statement_q', 'cash_flow_q', 'balance_sheet_q']:
#
#         df = fundamentals_dict[statement]
#
#         if period_ending is not None:
#             date_str = date.strftime('%Y-%m-%d')
#             df = df('date = @date_str')
#
#         # Transpose the DataFrame and reset the index to create a single Date column
#         df = df.T.reset_index()
#         df.rename(columns={"index": "date"}, inplace=True)
#         # Add period, share_id and currency columns
#         df.loc[:, ['report_type', 'share_id', 'reportedCurrency']] = 'Q' if statement.endswith('_q') else 'A', share_id, fundamentals_dict['reportedCurrency']
#         # Filter required columns
#         pg_columns = utils.pg_tables.get(statement.rstrip('_q')).keys()
#         required_columns = [col for col in pg_columns if col in df.columns]
#         df = df[required_columns]
#         # Rename columns
#         df = df.rename(columns=utils.pg_tables.get(statement.rstrip('_q')))
#         # Date formation to prevent an error for forex.request_usd_currency_value
#         df['date'] = df['date'].dt.strftime('%Y-%m-%d')
#         # Currency conversion
#         df['usd_exc'] = df.apply(forex.get_usd_exchange_rate, axis=1)
#         monetary_columns = df.columns.difference(utils.non_monetary_columns)
#         df[monetary_columns] = df[monetary_columns].div(df['usd_exc'], axis=0).mul(10000).round()
#         df.drop(columns=['usd_exc', 'reportedCurrency'], inplace=True)
#         df.sort_values(by=['date'], inplace=True, ascending=True)


if __name__ == "__main__":

    fingrep_service.get_and_insert_fundamentals(1, 'INV')
