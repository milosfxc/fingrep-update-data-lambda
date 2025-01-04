from collections import defaultdict
from email.policy import default
from os import PRIO_PGRP

import pandas as pd

import forex
import utils
from forex import request_usd_currency_value
if __name__ == "__main__":
    # Example dictionary
    currency_dict = defaultdict(
        dict,
        {'USD': {'2024-12-12': 0.4343, '2024-12-31': 0.85, '2024-12-30': 0.45}}
    )

    # Example DataFrame
    data = {
        'reportedCurrency': ['USD', 'USD', 'CAD', 'USD', 'USD'],
        'date': ['2024-12-31', '2024-12-30', '2021-12-31', '2024-12-12', '2019-12-31'],
        'value1': [10, 20, 30, 40, 50],
        'value2': [100, 200, 300, 400, 500],
    }
    df = pd.DataFrame(data)


    # Function to get multiplier based on reportedCurrency and date
    def get_usd_exchange_rate(row):
        reported_currency = row['reportedCurrency']
        if reported_currency == 'USD':
            return 1
        date = row['date']
        return forex.request_usd_currency_value(date, reported_currency)

    # Apply the multiplier to numeric columns
    df['usd'] = df.apply(get_usd_exchange_rate, axis=1)
    numeric_columns = df.select_dtypes(include='number').columns.difference(['usd'])
    df[numeric_columns] = df[numeric_columns].div(df['usd'], axis=0).mul(10000)
    print("Transformed DataFrame:")

    # data = {
    #     'A': [1, 2, 3],
    #     'B': [4, 5, 6],
    #     'C': [7, 8, 9],
    #     'D': [7, 8, 9],
    # }
    # df = pd.DataFrame(data)
    #
    # # Perform the operation on all columns except 'A'
    # columns_to_transform = df.columns.difference(['A', 'D', 'T'])
    # print(columns_to_transform)
    # df[columns_to_transform] = (df[columns_to_transform] / 0.44) * 10000
    #
    # print("Transformed DataFrame:")
    # print(df)
