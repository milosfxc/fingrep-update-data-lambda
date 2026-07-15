import datetime

from alert_service import run_scheduler
from config import FINGREP_1m_CHANNEL_ID
from telegram.sql_templates import conv3_gap10
from telegram_service import post_trade_alert
from sql_templates import conv3
from datetime import timedelta
from db_ops import fetch_ticker_id_map, get_max_temporal


def get_5m_conv3():
    sql_conv3 = conv3_gap10.format(table="timeframe_5m", temporal="datetime")
    datetime_5m = get_max_temporal("timeframe_5m", "datetime")
    params = {
        "temporal_0": datetime_5m,
        "temporal_1": datetime_5m - timedelta(minutes=5),
        "temporal_2": datetime_5m - timedelta(minutes=10),
    }
    ticker_id_map = fetch_ticker_id_map(sql_conv3, params)
    datetime_str = datetime_5m.strftime("%Y-%m-%dT%H:%M")
    for ticker in ticker_id_map.keys():
        post_trade_alert(image_path=None, message='conv3_gap10', link=f'https://fingrep.com/quote/{ticker}?timestamp={datetime_str}&tf=5m', channel_id=FINGREP_1m_CHANNEL_ID)

def get_d_conv3():
    sql_conv3 = conv3.format(table="d_timeframe", temporal="date")
    datetime_5m = get_max_temporal("d_timeframe", "date", offset=5)

    params = {
        "temporal_0": datetime_5m,
        "temporal_1": datetime_5m - timedelta(days=1),
        "temporal_2": datetime_5m - timedelta(days=2),
    }
    return fetch_ticker_id_map(sql_conv3, params)



if "__main__" == __name__:
    datetime_str = datetime.datetime(year=2025,month=5,day=22,hour=9,minute=45).strftime("%Y-%m-%dT%H:%M")
    ticker = 'MSFT'
    # This works
    # post_trade_alert(image_path=None,message='testing',link=f'https://fingrep.com/quote/{ticker}?timestamp={datetime_str}&tf=5m', channel_id=FINGREP_1m_CHANNEL_ID)
    # This doesn't work:
    run_scheduler(task=lambda : post_trade_alert(image_path=None,message='testing',link=f'https://fingrep.com/quote/{ticker}?timestamp={datetime_str}&tf=5m', channel_id=FINGREP_1m_CHANNEL_ID),
                  every_minutes=5,
                  second=15)