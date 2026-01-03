import os
from datetime import datetime, timezone, timedelta
from logging_utils import ColorFormatter,setup_logger
from ConnType import DBLocation
import logging
from edgar import set_identity
# stock market data config
days = 1 # Offset from current date
years = 5 # OHLCV data
date_from = datetime.utcnow().replace(tzinfo=timezone.utc).date() - timedelta(days=365 * years)
mb_historical = True # false updates market breadth for the current day, true updates for the last 100 days
insert_fundamentals = True # Insert fundamentals for new tickers
update_fundamentals = True # Update fundamentals for existing tickers
s3_upload = True
s3_upload_limit = 70
last_s3_upload = datetime.now(timezone.utc)
report_start_date = '2020-12-31'
limit = 2 # insertion limit
multi_threaded = False
threads_number = 10
thread_delay = 0.3 # Time delay between submitting a task
scale_factor = 10000
# Missing remote shares
missing_remote_shares = [4421, 4422, 4423, 4424, 4425, 4426, 4427, 4428, 4429, 4430, 4431, 4432, 4433, 4434, 4435, 4436, 4437, 4438, 4439, 4440, 4441, 4442, 4443, 4444, 4445, 4446, 4447, 4448, 4449, 4516, 4517, 4518, 4519, 4520, 4521, 4522, 4523, 4524, 4525, 4526, 4527, 4528, 4529, 4530, 4531, 4532, 4533, 4534, 4535, 4536, 4537, 4538, 4539, 4540, 4541, 4542, 4543]
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
