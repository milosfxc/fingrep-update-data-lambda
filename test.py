import datetime
import re

import yfinance
from PIL.XbmImagePlugin import XbmImageFile
from edgar import get_filings, set_identity, get_by_accession_number, Company
from datetime import timedelta

from fmpsdk import income_statement

import XBRLTagMapper
import db_ops
import pandas as pd
import re
import edgar_service
import fingrep_service
import fundamentals
import utils
import xbrl_utils
from fundamentals import request_fundamentals

# Set pandas to display all rows and columns
pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', None)  # To allow the console to use the full width

set_identity('milosfxc@gmail.com')




if __name__ == "__main__":
    sga = XBRLTagMapper.income_statement_gaap['selling_general_and_administrative_expense']
    rd = XBRLTagMapper.income_statement_gaap['research_and_development_expenses']
    dda = XBRLTagMapper.cash_flow_gaap['operating_da']
    print(dda.intersection(rd))


