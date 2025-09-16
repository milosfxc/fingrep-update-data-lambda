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


if __name__ == "__main__":
    fundamentals_service.get_company_fundamentals('HIT',6, config.report_start_date)
    # df = get_by_accession_number('0001213900-25-024561').xbrl().query().by_concept('us-gaap:CommonShares').to_dataframe()
    # df = get_by_accession_number('0001213900-25-024561').obj().financials.income_statement().to_dataframe().replace(['',np.nan],None)
    # df = get_by_accession_number('0001045810-25-000209').xbrl().query().by_concept('us-gaap:Revenue').by_instant_date('2025-07-27').to_dataframe()
    # print(df)