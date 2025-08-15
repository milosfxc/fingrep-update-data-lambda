import datetime
import os
import re
from typing import Union

import edgar
import numpy as np
import yfinance
from PIL.XbmImagePlugin import XbmImageFile
from edgar import get_filings, set_identity, get_by_accession_number, Company
from datetime import timedelta, timezone

import pandas as pd

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
def get_upsert():
    try:
        x = {1:2}
        x['b']
    except Exception as e:
        print(e.with_traceback())
        raise

if __name__ == "__main__":
    fundamentals_service.get_company_fundamentals('ADGM',14, config.report_start_date)
