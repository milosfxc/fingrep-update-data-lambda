import time

import mysql.connector
import pandas as pd
import db_ops
import fingrep_service

# Set pandas to display all rows and columns
pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', None)  # To allow the console to use the full width
if __name__ == '__main__':
    # print(db_ops.get_dolt_statement('ADC','income_statement', False))
    fingrep_service.get_and_insert_fundamentals(1, 'ATNF', '1690080')