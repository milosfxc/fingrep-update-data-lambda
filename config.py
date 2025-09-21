import os


from ConnType import DBLocation
# stock market data config
DAYS = 1 # Offset from current date
YEARS = 5 # OHLCV data
mb_historical = True # false updates market breadth for the current day, true updates for the last 100 days
insert_fundamentals = True # Insert fundamentals for new tickers
update_fundamentals = True # Update fundamentals for existing tickers
report_start_date = '2020-12-31'
LIMIT = 10 # insertion limit
MULTI_THREADED = True
THREADS_NUMBER = 20
THREAD_DELAY = 0.3 # Time delay between submitting a task
db_location = DBLocation.LOCAL
scale_factor = 10000
# database config
HOST=DBLocation.REMOTE
LOCAL_BIND_PORT=None
remote_connection_pool = None
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

# logger
import logging
logger = logging.getLogger('fingrep')
logger.setLevel(logging.ERROR)
logger.propagate = False
# Add handlers, formatters, etc.
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
# Suppress edgar.httpclient logger
edgar_logger = logging.getLogger('edgar')
edgar_logger.setLevel(logging.WARNING)  # Suppress INFO logs
# Suppress httpx logger
httpx_logger = logging.getLogger('httpx')
httpx_logger.setLevel(logging.WARNING)  # Suppress INFO logs
# Suppress httpxthrottlecache logger
httpxthrottlecache_logger = logging.getLogger('httpxthrottlecache')
httpxthrottlecache_logger.setLevel(logging.WARNING)
# Prevent propagation to the root logger
edgar_logger.propagate = False
httpx_logger.propagate = False
httpxthrottlecache_logger = False
