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
    print(db_ops.get_dolt_balance_sheet('MSFT'))
    # Example DataFrames
    df1 = pd.DataFrame({
        'date': ['2012-12-31', '2013-12-31'],
        'period': ['Year', 'Year'],
        'basic_eps': [-3.69, -0.62],
        'diluted_net_eps': [None, None]
    })

    df2 = pd.DataFrame({
        'date': ['2012-12-31', '2013-12-31'],
        'period': ['Year', 'Year'],
        'basic_eps': [-3.69, -0.62],
        'diluted_net_eps': [-5, -10],  # None/NaN in df2 for 2012-12-31
        'normalized_eps': [-4, 4]
    })

    # Step 1: Set 'date' as index for both DataFrames
    df1 = df1.set_index('date')
    df2 = df2.set_index('date')

    # Step 2: Fill NaN in df1 with df2 values (only where df1 has NaN)
    df1_updated = df1.combine_first(df2)

    # Step 3: Add missing columns from df2 (if any)
    missing_cols = df2.columns.difference(df1.columns)
    df_final = pd.concat([df1_updated, df2[missing_cols]], axis=1)

    # Reset index if needed
    df_final = df_final.reset_index()

    print(df_final)