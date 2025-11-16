from datetime import datetime, timezone
from utils import ColorFormatter,setup_logger
import logging
from ConnType import DBLocation
import os

indices_list = ['^SPX', '^IXIC', '^VIX', '^DJI', '^NYA', '^RUT']
DAYS_OFFSET = 120
CURRENT_UTC_DATE = datetime.now(timezone.utc).strftime('%Y-%m-%d')
CURRENT_UTC_DATETIME = datetime.now(timezone.utc)
INSERT_INDEX_DETAILS = True
INSERT_CURRENT_DAY = False
S3_UPLOAD = True
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
# logging config
# Logger handler
handler = logging.StreamHandler()
handler.setFormatter(ColorFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
# logger
logger = setup_logger(name='fingrep',level=logging.WARNING,handler=handler,propagate=False)
# aws logger
aws_logger = setup_logger(name='aws',level=logging.INFO,handler=handler,propagate=False)
