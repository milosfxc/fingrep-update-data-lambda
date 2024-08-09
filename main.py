import datetime
import logging

import pandas as pd

import utils
import yahoo_service
from db_ops import upsert_dataframe
import yahoo_api
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 400)
# df = yahoo_service.get_indices(['^IXIC', '^SPX'], date=datetime.date(2022, 8, 1))
# upsert_dataframe(df=df, table_name='daily_d_timeframe')
ticker = yahoo_service.get_index_details(utils.indices_list)
print(ticker)


