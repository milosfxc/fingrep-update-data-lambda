from massive import WebSocketClient
from massive.websocket.models import WebSocketMessage, Feed, Market
from typing import List

import fingrep_service
import utils

client = WebSocketClient(
	api_key="QQt28XYmrVXC4b_Cv2cWRWXlNJ5wVMy3",
	feed=Feed.Delayed,
	market=Market.Stocks
	)

# aggregates (per minute)
client.subscribe("AM.*") # single ticker
# client.subscribe("AM.*") # all tickers
# client.subscribe("AM.AAPL") # single ticker
# client.subscribe("AM.AAPL", "AM.MSFT") # multiple tickers

if __name__ == "__main__":
    fingrep_service.insert_minute_bars_for_ticker('NVDA', 2524, utils.get_utc_date(days=20, as_str=False), utils.get_utc_date(days=2, as_str=False))