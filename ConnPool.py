import datetime
from datetime import datetime, timedelta
import logging
import os
from sshtunnel import SSHTunnelForwarder
import config
import db_ops
from ConnType import DBLocation
from SSHTunnelManager import SSHTunnelManager


def perform_database_operations():
    """Perform the shared database operations."""
    print(db_ops.get_existing_tickers())
    print(db_ops.get_banned_tickers())
    print(db_ops.get_fillings_older_than_four_days())


if __name__ == "__main__":
    print(datetime.utcnow() - timedelta(days=config.DAYS))
    # try:
    #     if config.db_location == DBLocation.REMOTE:
    #         with SSHTunnelManager():
    #             perform_database_operations()
    #     else:  # Local database connection
    #         perform_database_operations()
    # except Exception as e:
    #     logging.error(f"An error occurred: {e}")