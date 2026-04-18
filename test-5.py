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

if __name__ == "__main__":
    import random
    import time
    from datetime import datetime, timedelta

    N = 2_000

    # Generate random data
    base_time = datetime(2026, 1, 1)

    rows = []
    for _ in range(N):
        share_id = random.randint(1, 1000)
        dt = base_time + timedelta(seconds=random.randint(0, 86400))
        rows.append({
            "share_id": share_id,
            "datetime": dt
        })

    # Warm-up (important for fair timing)
    for _ in range(3):
        sorted(rows, key=lambda r: (r["share_id"], r["datetime"]))

    # Actual timing
    start = time.perf_counter()

    sorted_rows = sorted(rows, key=lambda r: (r["share_id"], r["datetime"]))

    end = time.perf_counter()

    print(f"Sorting {N} rows took: {(end - start) * 1000:.3f} ms")