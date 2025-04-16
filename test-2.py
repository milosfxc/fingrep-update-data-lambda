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

    # Sample df1
    df1 = pd.DataFrame({
        'cik': [101, 102, 103],
        'other_column': ['A', 'B', 'C'],
        'partially_inserted': [False, False, False]
    })

    # Sample df2
    df2 = pd.DataFrame({
        'cik': [101, 102, 103],
        'partially_inserted': [True, False, True]
    })

    # Apply the mapping
    df1['partially_inserted'] = df1['cik'].map(df2.set_index('cik')['partially_inserted'])

    print(df1)
