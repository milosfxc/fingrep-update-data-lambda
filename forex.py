import logging
import utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)
from retry import retry
import requests
import os

CURRENCY_BEACON_API_KEY = os.getenv('CURRENCY_BEACON_API_KEY')
if not CURRENCY_BEACON_API_KEY:
    logger.error("CURRENCY_BEACON_API_KEY environment variable is not set.")

@retry(exceptions=requests.RequestException, tries=3, delay=2, backoff=2)
def request_usd_currency_value(date: str, currency) -> float:
    # Params
    params = {
        "api_key": CURRENCY_BEACON_API_KEY,  # Authentication
        "base": "USD",  # Base currency
        "date": date,  # Historical date in YYYY-MM-DD format
        "symbols": currency  # Optional: List of target currencies, comma-separated
    }
    response = requests.get("https://api.currencybeacon.com/v1/historical", params=params)
    if response.status_code != 200:
        raise requests.RequestException(f"request_usd_currency_value - API request failed with status code: {response.status_code}, response: {response.text}")
    try:
        return response.json()['response']['rates'][currency]
    except (KeyError, ValueError) as e:
        logger.error(f"request_usd_currency_value - Failed to parse API response: {str(e)}")
        raise


def get_usd_exchange_rate(row) -> float:
    reported_currency = row['currency']
    if reported_currency == 'USD':
        return 1
    date = row['date']
    return request_usd_currency_value(date, reported_currency)



