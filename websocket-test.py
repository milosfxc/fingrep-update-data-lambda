from datetime import datetime, timezone

from massive import WebSocketClient
from massive.websocket.models import WebSocketMessage, Feed, Market
from typing import List
import os

import db_ops
import utils


def handle_msg(msgs: List[WebSocketMessage]):
    for m in msgs:
        print(m)


if __name__ == "__main__":
    # ws = WebSocketClient(api_key= os.getenv("POLYGON_API_KEY"), subscriptions = ["AM.*"], feed=Feed.Delayed, market=Market.Stocks)
    # ws.run(handle_msg=handle_msg)
    # from fingrep_service import open_websocket_connection
    print(datetime.fromtimestamp(1773955500000/1000, timezone.utc))
    print(datetime.fromtimestamp(1773955560000/1000, timezone.utc))
    print(datetime.now(timezone.utc))