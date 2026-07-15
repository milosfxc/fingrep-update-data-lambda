from cffi.cffi_opcode import PRIM_FLOAT
from massive import websocket
import fingrep_service
from utils import get_utc_date
from db_ops import get_existing_tickers
from polygon_service import request_aggregate_bars
from datetime import datetime, timezone,date, timedelta,time
from db_ops_v2 import upsert_data_smart
from utils import us_market_open_utc
from config import  days
if __name__ == "__main__":
    existing_tickers = get_existing_tickers()

    fingrep_service.insert_minute_bars_for_date(existing_tickers, get_utc_date(13,False))

    # fingrep_service.insert_minute_bars({'MARA': existing_tickers.get('MARA')}, date(2026, 3, 4))
    # us_market_open = utils.us_market_open_utc(date(2025, 12, 16))
    # us_premarket_open = us_market_open - timedelta(hours=5.5)
    # us_market_close = us_market_open + timedelta(hours=6.5)
    # us_aftermarket_close = us_market_close + timedelta(hours=4) + timedelta(days=10)
    # date_str = int(us_premarket_open.timestamp() * 1000)
    # date_end = int(us_aftermarket_close.timestamp() * 1000)
    # bars = request_aggregate_bars(ticker="AAPL",timeframe="minute", multiplier=1,date_start=str(date_str),date_end=str(date_end),limit=50000)
    # ohlcv_list = bars['results']
    # insert_list = []
    # for ohlcv_dict in ohlcv_list:
    #     bar_datetime = datetime.fromtimestamp(ohlcv_dict['t'] / 1000, timezone.utc)
    #     insert_dict = {
    #         'datetime': bar_datetime,
    #         'abs_atr': None,
    #         'avg_volume': None,
    #         'close': int(ohlcv_dict['c'] * 10_000),
    #         'convergence2': None,
    #         'convergence3': None,
    #         'high': int(ohlcv_dict['h'] * 10_000),
    #         'low': int(ohlcv_dict['l'] * 10_000),
    #         'open': int(ohlcv_dict['o'] * 10_000),
    #         'rel_volume': None,
    #         'session': 0 if bar_datetime.time() < us_market_open.time() else 1 if bar_datetime.time() < us_market_close.time() else 2,
    #         'sma10': None,
    #         'volume': int(ohlcv_dict['v'] * 10_000),
    #         'vwap': int(ohlcv_dict['vw'] * 10_000),
    #         'share_id': 5
    #     }
    #     insert_list.append(insert_dict)
    # upsert_data_smart(insert_list,'timeframe_1m',{'share_id','datetime'})
