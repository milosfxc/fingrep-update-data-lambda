import config
import polygon_service
from _datetime import datetime, timezone, timedelta

if __name__ == '__main__':
    tickers = [
        "AAPL",  # Apple
        "MSFT",  # Microsoft
        "GOOGL",  # Alphabet (Google)
        "AMZN",  # Amazon
        "META",  # Meta Platforms
        "NVDA",  # Nvidia
        "TSLA",  # Tesla
        "BRK.B",  # Berkshire Hathaway
        "JPM",  # JPMorgan Chase
        "JNJ",  # Johnson & Johnson
        "V",  # Visa
        "PG",  # Procter & Gamble
        "XOM",  # Exxon Mobil
        "PFE",  # Pfizer
        "NFLX",  # Netflix
        "DIS",  # Disney
        "KO",  # Coca-Cola
        "PEP",  # PepsiCo
        "CSCO",  # Cisco Systems
        "INTC"  # Intel
    ]

    data = polygon_service.batch_requests(tickers,300,polygon_service.request_short_interest,'2025-09-30')