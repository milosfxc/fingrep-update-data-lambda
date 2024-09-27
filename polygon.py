import inspect
import os
import requests
import logging
from retry import retry

from utils import get_formatted_utc_date

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global variable to track retry attempts
retry_counter = 0

@retry(exceptions=requests.RequestException, tries=3, delay=2, backoff=2)
def request_grouped_daily_bars():
    global retry_counter
    url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{get_formatted_utc_date()}"

    params = {
        "adjusted": "true",
        "include_otc": "false",
        "apiKey": os.getenv("POLYGON_API_KEY")
    }

    method_name = inspect.currentframe().f_code.co_name
    try:
        response = requests.get(url, params=params)
        retry_counter += 1  # Increment retry count on each attempt

        if response.status_code == 200:
            data = response.json()
            retry_counter = 0  # Reset retry counter on success
            return data
        else:
            raise requests.RequestException(f"{method_name} - API request failed with status code: {response.status_code}")

    except requests.RequestException as e:
        if retry_counter >= 3:  # Only log after the third attempt
            logger.error(f"{method_name} - Exception occurred on the third try: {str(e)}")
            retry_counter = 0  # Reset the counter after third attempt
        raise


@retry(exceptions=requests.RequestException, tries=3, delay=2, backoff=2)
def request_aggregate_daily_bars(ticker, date_from, limit):
    global retry_counter
    url = (f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{date_from}/{get_formatted_utc_date()}"
           f"?adjusted=true&sort=asc&limit={limit}")
    params = {
        "apiKey": os.getenv("POLYGON_API_KEY")
    }


    method_name = inspect.currentframe().f_code.co_name
    try:
        response = requests.get(url, params=params)
        retry_counter += 1

        if response.status_code == 200:
            data = response.json()
            retry_counter = 0
            return data
        else:
            raise requests.RequestException(f"{method_name} - API request failed with status code: {response.status_code}")

    except requests.RequestException as e:
        if retry_counter >= 3:  # Only log after the third attempt
            logger.error(f"{method_name} - Exception occurred on the third try: {str(e)}")
            retry_counter = 0  # Reset the counter after third attempt
        raise


@retry(exceptions=requests.RequestException, tries=3, delay=2, backoff=2)
def request_ticker_details_v3(ticker):
    global retry_counter
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"

    params = {
        "apiKey": os.getenv("POLYGON_API_KEY")
    }

    method_name = inspect.currentframe().f_code.co_name

    try:
        response = requests.get(url, params=params)
        retry_counter += 1

        if response.status_code == 200:
            data = response.json()
            retry_counter = 0
            return data['results']
        else:
            raise requests.RequestException(f"{method_name} - API request failed with status code: {response.status_code}")

    except requests.RequestException as e:
        if retry_counter >= 3:  # Only log after the third attempt
            logger.warning(f"{method_name} - Exception occurred on the third try: {str(e)}")
            retry_counter = 0  # Reset the counter after third attempt
        raise


@retry(exceptions=requests.RequestException, tries=3, delay=2, backoff=2)
def request_splits():
    global retry_counter

    url = (f"https://api.polygon.io/v3/reference/splits?execution_date={get_formatted_utc_date()}"
           f"&reverse_split=true&limit=25")

    params = {
        "apiKey": os.getenv("POLYGON_API_KEY")
    }
    try:
        response = requests.get(url, params=params)
        retry_counter += 1

        if response.status_code == 200:
            data = response.json()
            retry_counter = 0
            return data['results']
        else:
            raise requests.RequestException(f"request_splits - API request failed with status code: {response.status_code}")
    except requests.RequestException as e:
        if retry_counter >= 3:  # Only log after the third attempt
            logger.warning(f"request_splits - Exception occurred on the 3rd try: {str(e)}")
            retry_counter = 0  # Reset the counter after third attempt
        raise