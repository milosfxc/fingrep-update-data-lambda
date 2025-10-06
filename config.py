import os
from utils import ColorFormatter,setup_logger
from ConnType import DBLocation
import logging
from edgar import set_identity
# stock market data config
days = 7 # Offset from current date
years = 5 # OHLCV data
mb_historical = True # false updates market breadth for the current day, true updates for the last 100 days
insert_fundamentals = True # Insert fundamentals for new tickers
update_fundamentals = False # Update fundamentals for existing tickers
s3_upload = True
report_start_date = '2020-12-31'
limit = 1 # insertion limit
multi_threaded = False
threads_number = 10
thread_delay = 0.3 # Time delay between submitting a task
scale_factor = 10000
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

# Logger handler
handler = logging.StreamHandler()
handler.setFormatter(ColorFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
# logger
logger = setup_logger(name='fingrep',level=logging.ERROR,handler=handler,propagate=False)
# aws logger
aws_logger = setup_logger(name='aws',level=logging.INFO,handler=handler,propagate=False)
# Suppress edgar.httpclient logger
edgar_logger = setup_logger(name='edgar',level=logging.ERROR,handler=handler,propagate=False)
# Suppress httpx logger
httpx_logger = setup_logger(name='httpx',level=logging.ERROR,handler=handler,propagate=False)
# Suppress httpxthrottlecache logger
httpxthrottlecache_logger = setup_logger(name='httpxthrottlecache',level=logging.ERROR,handler=handler,propagate=False)
