import datetime

import clickhouse_service
from config import FINGREP_1m_CHANNEL_ID
from telegram.sql_templates import conv3_gap10
from telegram_service import post_trade_alert
from sql_templates import conv3, top_5p_change_conv3
from datetime import timedelta, timezone
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


def top_5p_change_30m_conv3():
    dt_utc = datetime.datetime.now(tz=timezone.utc)
    dt_utc = dt_utc.replace(month=6, day=11, hour=18, minute=55)
    if dt_utc.minute >= 30:
        dt_utc = dt_utc.replace(minute=30, second=0, microsecond=0)
    else:
        dt_utc = dt_utc.replace(minute=00, second=0, microsecond=0)
    dt_start_str = (dt_utc - timedelta(days=60)).strftime("%Y-%m-%d %H:%M:%S")
    dt_end_str = dt_utc.strftime("%Y-%m-%d %H:%M:%S")
    ticker_ids = clickhouse_service.query_by_template(template=top_5p_change_conv3, template_params={'timeframe': '30m', 'dt_start_str': dt_start_str, 'dt_end_str': dt_end_str})
    if ticker_ids:
        link_text = f'{len(ticker_ids)} tickers found'
        for i in ticker_ids:
            link_text += f'\nhttps://fingrep.com/quote/{i[0]}?timestamp={i[1].strftime("%Y-%m-%dT%H:%M:%S")}&tf=30m'
        post_trade_alert(image_path=None,message='30m convergence',link=link_text, channel_id=FINGREP_1m_CHANNEL_ID)


if "__main__" == __name__:
    top_5p_change_30m_conv3()