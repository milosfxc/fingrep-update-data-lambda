import datetime
import logging

import pandas as pd

import db_ops
import utils
import yahoo_service
from db_ops import upsert_dataframe
import yahoo_api
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 400)
# df = yahoo_service.get_indices(['^IXIC', '^SPX'], date=datetime.date(2022, 8, 1))
# upsert_dataframe(df=df, table_name='daily_d_timeframe')
foreign_keys = db_ops.get_foreign_keys()
currency_mapping = foreign_keys.get('currencies')
ticker = yahoo_service.get_index_details(utils.indices_list, currency_mapping)
db_ops.upsert_dataframe(ticker, 'indices')