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
    x = {'a': 1, 'b': 2}
    if 'a' in x.keys():
        print(x)

