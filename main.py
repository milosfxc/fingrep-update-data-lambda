import datetime
from datetime import timedelta

import pandas as pd
from sshtunnel import BaseSSHTunnelForwarderError

import db_ops
import config
import yahoo_service
from ConnType import DBLocation
from SSHTunnelManager import SSHTunnelManager

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 400)

def update_indices():
        # Foreign keys
        foreign_keys = db_ops.get_foreign_keys()
        # Insert index details
        if config.INSERT_INDEX_DETAILS:
                ticker = yahoo_service.get_index_details(config.indices_list, foreign_keys.get('currencies'))
                db_ops.upsert_dataframe(ticker, 'indices')

        # Foreign keys
        foreign_keys = db_ops.get_foreign_keys()
        indices_mapping = foreign_keys.get('indices')
        # OHLCV data
        date = datetime.date.today() - datetime.timedelta(days=config.DAYS_OFFSET)
        ohlcv_data = yahoo_service.get_indices_ohlcv(config.indices_list, date=date, indices_mapping=indices_mapping)
        # Removes all rows that aren't current date
        if config.INSERT_CURRENT_DAY:
                ohlcv_data = ohlcv_data[ohlcv_data['date'] == date.today().strftime('%Y-%m-%d')]
        # Insert if not empty
        if not ohlcv_data.empty:
                db_ops.upsert_dataframe_composite_id(ohlcv_data, 'indices_d_timeframe')

if __name__ == '__main__':
        if config.db_location == DBLocation.REMOTE:
                try:
                        with SSHTunnelManager():
                                update_indices()
                except BaseSSHTunnelForwarderError as ssh_error:
                        db_ops.logger.error(f"SSH tunnel error occurred: {ssh_error}")
                        raise
        else:
                update_indices()