import inspect
import os
import urllib
import requests
from retry import retry
import config
from utils import get_utc_date
from config import logger

# Global variable to track retry attempts
retry_counter = 0

@retry(exceptions=requests.RequestException, tries=3, delay=2, backoff=2)
def request_grouped_daily_bars(date: str):
    url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}"
    params = {
        "adjusted": "true",
        "include_otc": "false",
        "apiKey": os.getenv("POLYGON_API_KEY")
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()

    except requests.HTTPError as e:
        if e.response.status_code == 429:
            retry_after = int(e.response.headers.get("Retry-After", 1))
            logger.warning(f"Rate limited. Retry after {retry_after}s")
        raise requests.RequestException(f"API error: {str(e)}")
    except Exception as e:
        logger.critical(f"Unexpected error: {str(e)}", exc_info=True)
        raise

@retry(exceptions=requests.RequestException, tries=3, delay=2, backoff=2)
def request_all_tickers(date: str):
    all_tickers = []
    url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&date={date}&active=true&order=asc&limit=1000&sort=ticker"
    params = {
        "apiKey": os.getenv("POLYGON_API_KEY")
    }
    while url:
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            response_json = response.json()
            url = response_json['next_url'] if 'next_url' in response_json else None
            all_tickers.extend(response_json['results'])

        except requests.HTTPError as e:
            if e.response.status_code == 429:
                retry_after = int(e.response.headers.get("Retry-After", 1))
                logger.warning(f"Rate limited. Retry after {retry_after}s")
            raise requests.RequestException(f"API error: {str(e)}")
        except Exception as e:
            logger.critical(f"Unexpected error: {str(e)}", exc_info=True)
            raise
    return all_tickers


@retry(exceptions=requests.RequestException, tries=3, delay=2, backoff=2)
def request_aggregate_daily_bars(ticker, date_from, limit):
    global retry_counter
    url = (f"https://api.massive.com/v2/aggs/ticker/{ticker}/range/1/day/{date_from}/{get_utc_date(days=config.days)}"
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

    url = (f"https://api.polygon.io/v3/reference/splits?execution_date={get_utc_date(days=config.days)}"
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

@retry(exceptions=requests.RequestException, tries=3, delay=2, backoff=2)
def request_short_interest(tickers: list, date:str, date_operator:str = ''):
    global retry_counter

    tickers_encoded = urllib.parse.quote(','.join(tickers))
    url = f"https://api.polygon.io/stocks/v1/short-interest?settlement_date{date_operator}={date}&ticker.any_of={tickers_encoded}&limit=25000&sort=settlement_date.asc"
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
            raise requests.RequestException(
                f"request_short_interest - API request failed with status code: {response.status_code}")
    except requests.RequestException as e:
        if retry_counter >= 3:  # Only log after the third attempt
            logger.warning(f"request_short_interest - Exception occurred on the 3rd try: {str(e)}")
            retry_counter = 0  # Reset the counter after third attempt
        raise


@retry(exceptions=requests.RequestException, tries=3, delay=2, backoff=2)
def request_short_volume(tickers: list, date:str, date_operator:str = ''):
    global retry_counter

    tickers_encoded = urllib.parse.quote(','.join(tickers))
    url = f"https://api.massive.com/stocks/v1/short-volume?ticker.any_of={tickers_encoded}&date{date_operator}={date}&limit=25000&sort=date.asc"
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
            raise requests.RequestException(
                f"request_short_volume - API request failed with status code: {response.status_code}")
    except requests.RequestException as e:
        if retry_counter >= 3:  # Only log after the third attempt
            logger.warning(f"request_short_volume - Exception occurred on the 3rd try: {str(e)}")
            retry_counter = 0  # Reset the counter after third attempt
        raise


def batch_requests(tickers:list, request_function: callable,batch_size:int, date:str, date_operator:str = '') -> list[dict] | None:
    if not tickers:
        logger.warning(f"batch_requests_{request_function.__name__} - No tickers provided for batch requests")
        return None
    data = []
    for i in range(0, len(tickers), batch_size):
        batch =  tickers[i:i+batch_size]
        try:
            if requested_data := request_function(tickers=batch, date=date, date_operator=date_operator):
                data.extend(requested_data)
        except requests.RequestException as e:
            logger.error(f"batch_requests_{request_function.__name__} - Exception occurred on the 3rd try for the batch of tickers {batch}: \n{e}")
    return data


@retry(exceptions=requests.RequestException, tries=3, delay=2, backoff=2)
def request_aggregate_bars(ticker:str, timeframe:str, date_start:str, date_end:str, limit:int):
    global retry_counter
    url = (f"https://api.massive.com/v2/aggs/ticker/{ticker}/range/1/{timeframe}/{date_start}/{date_end}"
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