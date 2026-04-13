import os
from datetime import datetime, timezone, timedelta
from logging_utils import ColorFormatter,setup_logger
from ConnType import DBLocation
import logging
from edgar import set_identity
# stock market data config
days = 82 # Offset from current date
years = 5 # OHLCV daily data
days_1m = 5 # OHLCV 1m data
date_from = datetime.utcnow().replace(tzinfo=timezone.utc).date() - timedelta(days=365 * years)
update_existing_tickers_1m_timeframe = False
insert_new_tickers_1m_timeframe = False
insert_new_tickers = False
mb_historical = False # false updates market breadth for the current day, true updates for the last 100 days
market_metrics = False
insert_fundamentals = False # Insert fundamentals for new tickers
update_fundamentals = False # Update fundamentals for existing tickers
s3_upload = False
s3_upload_limit = 70
last_s3_upload = datetime.now(timezone.utc)
report_start_date = '2020-12-31'
limit = 100 # insertion limit
multi_threaded = False
threads_number = 10
thread_delay = 0.3 # Time delay between submitting a task
scale_factor = 10000
# Missing remote shares
missing_remote_shares = []
# EDGAR Identity
set_identity('milosfxc@gmail.com')
# database config
db_location = DBLocation.LOCAL
remote_connection_pool = None
LOCAL_BIND_PORT=None
BASTION_IP = os.getenv("FINGREP_BASTION_IP")
BASTION_PORT = int(os.getenv("FINGREP_BASTION_PORT"))
BASTION_USER = os.getenv("FINGREP_BASTION_USER")
BASTION_KEY = os.getenv("FINGREP_BASTION_KEY")
REMOTE_DB_HOST = os.getenv("FINGREP_REMOTE_DB_HOST")
LOCAL_DB_HOST = os.getenv("FINGREP_LOCAL_DB_HOST")
DB_NAME = os.getenv("FINGREP_DB")
DB_USER = os.getenv("FINGREP_DB_USER")
DB_PASSWORD = os.getenv("FINGREP_DB_PASS")
DB_PORT = 5432
#POLYGON
POLYGON_API_KEY = os.getenv("FINGREP_POLYGON_API_KEY")

# Logger handler
handler = logging.StreamHandler()
handler.setFormatter(ColorFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
# logger
logger = setup_logger(name='fingrep',level=logging.WARNING,handler=handler,propagate=False)
# aws logger
aws_logger = setup_logger(name='aws',level=logging.INFO,handler=handler,propagate=False)
# Suppress edgar.httpclient logger
edgar_logger = setup_logger(name='edgar',level=logging.ERROR,handler=handler,propagate=False)
# Suppress httpx logger
httpx_logger = setup_logger(name='httpx',level=logging.ERROR,handler=handler,propagate=False)
# Suppress httpxthrottlecache logger
httpxthrottlecache_logger = setup_logger(name='httpxthrottlecache',level=logging.ERROR,handler=handler,propagate=False)
