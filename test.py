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
    # filings = edgar.get_filings(filing_date='2025-09-02', form=['10-Q'])
    # download_filings(filing_date='2025-09-02', filings=filings,compress=False)

    # filings = edgar.get_filings(filing_date='2025-09-02', form=['10-Q']).to_pandas()
    # print(filings)
    edgar.download_edgar_data()
    online_time = time.perf_counter()
    bs = edgar.get_by_accession_number(accession_number='0001166388-25-000117')
    print(time.perf_counter() - online_time)
    print(bs)
    # edgar.use_local_storage(True)
    # offline_time = time.perf_counter()
    # bs = edgar.get_by_accession_number(accession_number='0001817358-25-000158').xbrl().statements.balance_sheet().to_dataframe()
    # print(time.perf_counter() - offline_time)
    # print(bs)
    # download_filings(filing_date='2025-01-01:2025-09-19', filings=filings)
    # yahoo_service.get_company_fundamentals(ticker='AEP', share_id=71, nearby_report_date=pd.Timestamp(year=2025, month=3, day=31))

    # df = edgar.get_by_accession_number('0000004904-25-000027').xbrl().statements.income_statement().to_dataframe()
    # df_dict = get_by_accession_number('0000012208-25-000052')
    # df = get_by_accession_number('0001213900-25-024561').obj().financials.income_statement().to_dataframe().replace(['',np.nan],None)
    # df = get_by_accession_number('0001045810-25-000209').xbrl().query().by_concept('us-gaap:Revenue').by_instant_date('2025-07-27').to_dataframe()]
    # df_dict = df.to_dict()
    # with open('output.json', "w") as f:
    #     json.loads(df_dict, f, indent=4)  # indent for pretty printing
    #
    # with open('output.json.json', 'r') as file:
    #     df_dict = json.load(file)

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

