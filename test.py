import datetime
import json
import os
import random
import re
import time
import edgar
from typing import Union
from edgar import set_identity, Company, download_filings, Filing
import numpy as np
import yfinance
from PIL.XbmImagePlugin import XbmImageFile
from datetime import timedelta, timezone

import pandas as pd
from edgar.xbrl.standardization import standardize_statement

import config
import db_ops
import fundamentals_service
import yahoo_service

# Set pandas to display all rows and columns
pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', None)  # To allow the console to use the full width

set_identity('milosfxc@gmail.com')
# set_5 = set(yahoo_service.request_fundamentals('MARA')['cash_flow'].index.tolist())
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor


def process_ticker(number, index):
    time.sleep(random.randint(1, 15))
    x = add(number=number)
    print(f"Value calculated {x} for index {index}")
    return index

def add(number: int) -> int:
    return number + 1


if __name__ == "__main__":
    x1 = time.perf_counter()
    fundamentals_service.get_company_fundamentals('WEYS',60,config.report_start_date)



    print(time.perf_counter() - x1)

    # Using ThreadPoolExecutor
    # with ThreadPoolExecutor(max_workers=2) as executor:
    #     results = []
    #     for i, number in enumerate(list(range(1,20))):
    #         if i >= config.LIMIT:
    #             break
    #         executor.submit(process_ticker, number, i)
    #         print(i)

import concurrent.futures
from concurrent.futures import ThreadPoolExecutor


    # def process_ticker(new_ticker):
    #     fingrep_service.get_new_ticker_data_and_insert(new_ticker, finviz_df)
    #     return new_ticker
    #
    #
    # # Using ThreadPoolExecutor
    # with ThreadPoolExecutor(max_workers=2) as executor:
    #     results = []
    #     for i, new_ticker in enumerate(tickers_list):
    #         if i >= config.LIMIT:
    #             break
    #         executor.submit(fingrep_service.get_new_ticker_data_and_insert, new_ticker, finviz_df)
    #         print(i)
# Here I have some other code that I want to run after the threaded code above

