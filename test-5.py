from massive import WebSocketClient
from massive.websocket.models import WebSocketMessage, Feed, Market
from typing import List

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

def handle_msg(msgs: List[WebSocketMessage]):
    count = 0
    count += len(msgs)
    print(count)

if __name__ == "__main__":
    # print messages
    client.run(handle_msg)