import datetime
import logging
import pandas as pd
import db_ops
import utils
import yahoo_service

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 400)

# Foreign keys
foreign_keys = db_ops.get_foreign_keys()

# Index details
ticker = yahoo_service.get_index_details(utils.indices_list, foreign_keys.get('currencies'))
db_ops.upsert_dataframe(ticker, 'indices')

# Foreign keys
foreign_keys = db_ops.get_foreign_keys()
indices_mapping = foreign_keys.get('indices')

# OHLCV data
date = datetime.date.today() - datetime.timedelta(days=utils.DAYS_OFFSET)
ohlcv_data = yahoo_service.get_indices_ohlcv(utils.indices_list, date=date, indices_mapping=indices_mapping)
db_ops.upsert_dataframe_composite_id(ohlcv_data, 'indices_d_timeframe')
