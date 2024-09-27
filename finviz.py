import inspect

import requests
from bs4 import BeautifulSoup
import logging
logger = logging.getLogger(__name__)



def get_sic(ticker):
    finviz_url = f"https://finviz.com/quote.ashx?t={ticker}&ty=c&p=d&b=1"
    headers = {'User-Agent': 'Mozilla/5.0'}
    method_name = inspect.currentframe().f_code.co_name
    try:
        page = requests.get(url=finviz_url, headers=headers, timeout=10)  # Timeout set to 10 seconds
        page.raise_for_status()  # Raise an HTTPError for bad requests
        soup = BeautifulSoup(page.content, 'html.parser')

        quote_links_div = soup.find('div', class_='quote-links')
        links_text = [link.text.strip() for link in quote_links_div.find_all('a')][:3]

        sic_data = {
            'sector': links_text[0],
            'industry': links_text[1],
            'country': links_text[2]
        }
        return sic_data
    except requests.exceptions.Timeout:
        logger.info(f"{method_name} - request timeout")
        return dict()
    except requests.exceptions.RequestException as e:
        logger.info(f"{method_name} - request exception: {e}")
        return dict()