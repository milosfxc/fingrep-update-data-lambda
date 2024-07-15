import datetime
import logging

import pandas as pd
from db_ops import upsert_dataframe
from edgar import get_trading_info
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
