import datetime
import time

import numpy as np
import pandas as pd

import utils
from db_ops import upsert_dataframe
from edgar import get_trading_info
import logging
from fmp import get_fundamentals_v2

logger = logging.getLogger(__name__)


def get_and_insert_fundamentals(cik: str, share_id: int, currency_id: int, ticker: str, period: str,
                                date: datetime = None):
    for statement in ['income_statement', 'cash_flow', 'balance_sheet']:
        response_json = get_fundamentals_v2(statement=statement, ticker=ticker, period='annual')
        if response_json is None or 'Error Message' in response_json:
            time.sleep(3)
            response_json = get_fundamentals_v2(statement=statement, ticker=ticker, period=period)
            if response_json is None or 'Error Message' in response_json:
                logger.error(f"{get_and_insert_fundamentals.__name__}: No data returned for {statement} "
                             f"and ticker {ticker}. The response: {response_json}.")
                break
        try:
            df = pd.DataFrame(response_json)
            if date is not None:
                date_str = date.strftime('%Y-%m-%d')
                df = df.query('date > @date_str')
            if 'cik' not in df.columns or df.iloc[0]['cik'] != cik.zfill(10):
                logger.error(f"CIK {cik} mismatch if CIK column is present here: {df.columns}")
                break
            df.loc[:, ['period', 'share_id', 'currency_id']] = 'A', share_id, currency_id

            df = df[utils.pg_tables.get(statement).keys()]
            df = df.rename(columns=utils.pg_tables.get(statement))
            df.sort_values(by=['date'], inplace=True, ascending=True)
            upsert_dataframe(df, statement)
        except Exception as e:
            logger.error(f"Error preparing for insert fundamentals for ticker {ticker}")


def get_and_insert_trading_info(cik: str, share_id: int, date: datetime.date):
    try:
        df = get_trading_info(cik=cik, date=date)
        if df is not None:
            df.reset_index(inplace=True)
            df.rename(columns={'end': 'date'}, inplace=True)
            df.loc[:, ['share_id']] = share_id
            upsert_dataframe(df, 'trade_info')
    except Exception as e:
        logger.error(
            f"Error occurred while trying to rename and reindex data frame for upsert into database for CIK {cik} "
            f"and share ID {share_id}: {e}")
