import csv
import time
from os import WCONTINUED
from time import sleep, struct_time
from typing import Optional, Dict
from pprint import pprint
from zoneinfo import ZoneInfo

from edgar.display.formatting import accepted_time_text
from markdown_it.common.utils import escapeHtml
from datetime import datetime, timedelta, date
import config
import pandas as pd

import fingrep_service
from config import POLYGON_API_KEY
import db_ops
import edgar_service_v2
import fundamentals_service
import polygon_service
import utils
from utils import get_utc_date

pd.set_option('display.max_rows', None)            # Show all rows
pd.set_option('display.max_columns', None)         # Show all columns
pd.set_option('display.max_colwidth', 50)        # No truncation in cell values
pd.set_option('display.width', 1000)               # Wide console width
pd.set_option('display.expand_frame_repr', False)  # <<< THIS one disables line wrapping!
pd.set_option('display.float_format', '{:,.2f}'.format)
import edgar
from pprint import pprint
from datetime import datetime, timedelta, time, timezone, UTC


import plotly.graph_objects as go


import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import pandas as pd


import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime


def plot_candlestick_with_volume(
    df: pd.DataFrame,
    ticker: str,
    timeframe: str,
    vertical_line: int | None = None,
) -> go.Figure:

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.75, 0.25],
        vertical_spacing=0.03,
    )

    # ---- Candlesticks ----
    fig.add_trace(
        go.Candlestick(
            x=df.index,
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"],
            name="Price",
        ),
        row=1,
        col=1,
    )

    # ---- Volume ----
    colors = np.where(df["close"] >= df["open"], "green", "red")

    fig.add_trace(
        go.Bar(
            x=df.index,
            y=df["volume"],
            marker_color=colors,
            name="Volume",
        ),
        row=2,
        col=1,
    )

    # ---- Vertical line marker ----
    if vertical_line is not None:
        fig.add_vline(
            x=vertical_line,
            line_width=1.5,
            line_dash="dash",
            line_color="yellow",
            annotation_text="Event",
            annotation_position="top left",
        )

    fig.update_layout(
        title=f"{ticker} – {timeframe}",
        template="plotly_dark",
        xaxis_rangeslider_visible=False,
        height=800,
        showlegend=False,
    )

    return fig




def normalize_polygon_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename Polygon OHLCV columns and convert timestamp.

    Args:
        df: Raw Polygon DataFrame

    Returns:
        Clean DataFrame ready for candlestick charts
    """

    df = df.rename(
        columns={
            "o": "open",
            "c": "close",
            "h": "high",
            "l": "low",
            "v": "volume",
            "vw": "vwap",
            "t": "timestamp",
            "n": "trades",
        }
    )

    # Convert milliseconds → datetime
    df["timestamp"] = (
        pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        .dt.tz_convert("US/Eastern")
    )

    # Use timestamp as index (Plotly prefers this)
    df.set_index("timestamp", inplace=True)

    # Ensure numeric types (important for Plotly & indicators)
    numeric_cols = ["open", "high", "low", "close", "volume", "vwap"]
    df[numeric_cols] = df[numeric_cols].astype(float)

    return df


def get_exhibit_text(accession_number:str):
    filing = edgar.get_by_accession_number(accession_number=accession_number)
    exhibits = filing.attachments.exhibits
    for ex in iter(exhibits):
        if '99.1' in ex.description:
            if exhibit_text := ex.text():
                exhibit_text = exhibit_text.lower()
                header_sentences = exhibit_text.split('\n')[:6]
                header = ''.join(header_sentences)
                if 'offering' in header and 'notes' in header:
                    tickers = filing.get_entity().tickers
                    header = filing.header
                    if not header: continue
                    accepted_time = header.acceptance_datetime
                    if not tickers or not accepted_time: continue
                    display_chart(tickers[0], accepted_time)

            #
            # if 'convertible notes' in exhibit_text or 'exchangeable notes' in exhibit_text:
            #     if 'p.m.' in exhibit_text or 'a.m.' in exhibit_text:
            #         print(f'{filing.get_entity().tickers} {accession_number} {filing.filing_date} {'weighted average price' in exhibit_text}')
            #     else:
            #         print(f'{filing.get_entity().tickers} {accession_number} {filing.filing_date},time not specified')
def get_convertible_notes_offerings(date:str) -> pd.DataFrame:
    ciks = set(db_ops.get_cached_foreign_keys()['ticker_and_share_id_by_cik'].keys())
    df_filings = edgar.get_filings(filing_date=date, form='8-K',amendments=False).to_pandas()
    if df_filings.empty: return pd.DataFrame()
    for row in df_filings.itertuples(index=True):
        if row.cik in ciks:
            get_exhibit_text(row.accession_number)
    return df_filings



def display_chart(ticker:str,acceptance_datetime:str):

    data = polygon_service.request_aggregate_bars(
        ticker=ticker,
        date_start=acceptance_datetime.date().strftime('%Y-%m-%d'),
        date_end=(acceptance_datetime + timedelta(days=1)).date().strftime('%Y-%m-%d'),
        timeframe="minute",
        limit=50000
    )
    if not data or not data['results']:
        print('Missing polygon data')
        return
    str_accepted_datetime = acceptance_datetime.strftime('%Y-%m-%d %H:%M:%S')
    vert_line = int(acceptance_datetime.timestamp() * 1000)
    df = pd.DataFrame.from_dict(data['results'], orient='columns')
    fig = plot_candlestick_with_volume(normalize_polygon_ohlcv(df), ticker=ticker + "-" + str_accepted_datetime, timeframe="1m",vertical_line=vert_line)
    fig.show()

if __name__ == "__main__":

    # for i in range(1,31):
    #     date_str = date(2025, 11, i).strftime('%Y-%m-%d')
    #     get_convertible_notes_offerings(date_str)
    # date_from = datetime.now(UTC).date() - timedelta(days=365 * config.years)
    # fingrep_service.get_and_insert_aggregated_bars('GUT',9248,date_from, 5000)
    # fundamentals_service.get_company_fundamentals('GUT',9248,config.report_start_date)
    df = pd.read_csv('/home/milosfxc/Desktop/ids.csv')
    arr = df['share_id'].values.tolist()
    print(f"missing_remote_shares = {arr[1500:]}")