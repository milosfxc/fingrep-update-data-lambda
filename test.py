import datetime
import io
import json
import os
import random
import re
import time

import boto3
import edgar
from typing import Union
from edgar import set_identity, Company, download_filings, Filing
import numpy as np
import yfinance
from PIL.XbmImagePlugin import XbmImageFile
from datetime import timedelta, timezone

import pandas as pd
from edgar.xbrl.standardization import standardize_statement

import aws_service
import config
import db_ops
import fundamentals_service
import utils
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
    utc_datetime = utils.get_utc_date(config.days)
    # Initialize S3 client with your environment variables
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_FINGREP_DB_DATA_KEY'),
        aws_secret_access_key=os.getenv('AWS_FINGREP_DB_DATA_SECRET'),
    )
    s3_client.put_object(
        Bucket='fingrep',
        Key='db_data/end.txt',
        ContentType='text',
        Metadata={
            'timestamp': utc_datetime
        }
    )
    # print(file_csv)
    # print(file_csv)
    # df_csv = pd.read_csv('/home/milos/Desktop/data.csv')
    # print(db_ops.query_data_as_csv('shares', {'id': list({3})}))
    # csv_content = aws.read_csv_from_file('/home/milos/Desktop/data.csv')
    # aws.upsert_from_csv_data(csv_content, 'd_timeframe', {'share_id','date'})
    # df = edgar.get_by_accession_number('0001379785-22-000032').obj().financials
    # df2 = edgar.get_by_accession_number('0001410578-22-000345').xbrl()
    # df3 = edgar.get_by_accession_number('0001410578-23-000248').xbrl()
    # print(df1.entity_info)
    # print(df2.entity_info)
    # print(df3.entity_info)
    # print(df1.period_of_report)
    # print(df2.period_of_report)
    # print(df3.period_of_report)


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

