import os

from ConnType import DBLocation
# stock market data config
DAYS = 2
YEARS = 5
mb_historical = True # false updates market breadth for the current day, true updates for the last 100 days
fundamentals = True
LIMIT = 5
db_location = DBLocation.LOCAL
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
logger.setLevel(logging.INFO)
logger.propagate = False

# Add handlers, formatters, etc.
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

