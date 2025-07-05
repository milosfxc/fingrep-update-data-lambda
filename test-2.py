import calendar
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
# Set pandas to display all rows and columns
pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', None)  # To allow the console to use the full width
import db_ops

if __name__ == '__main__':

    data = {
        'cik': 10,
        'other_column': 'C',
        'partially_inserted':False
    }
    # Sample df1
    df1 = pd.DataFrame( data={'value': data.values()}, index=[x for x in data.keys()])

    # Sample df2
    df2 = pd.DataFrame({
        'cik': [101, 102, 103],
        'partially_inserted': [True, False, True]
    })


    x = 0
    if x:
        print('hello')
    else:
        print('Hallo')
