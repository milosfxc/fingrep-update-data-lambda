import time

import pandas as pd
pd.set_option('display.max_rows', None)            # Show all rows
pd.set_option('display.max_columns', None)         # Show all columns
pd.set_option('display.max_colwidth', 50)        # No truncation in cell values
pd.set_option('display.width', 1000)               # Wide console width
pd.set_option('display.expand_frame_repr', False)  # <<< THIS one disables line wrapping!
pd.set_option('display.float_format', '{:,.2f}'.format)
import edgar

import config
import fundamentals_service
import utils
from edgar import get_filings
if __name__ == "__main__":
    fundamentals_service.get_company_fundamentals('FLGT', 836, config.report_start_date)
    # start_time = time.perf_counter()
    #
    # filing = edgar.get_by_accession_number('0001562762-25-000278')
    # df_xbrl = filing.xbrl().query().by_dimension(None).to_dataframe().to_dict()
    # print(df_xbrl)
    #
    # print(time.perf_counter() - start_time)