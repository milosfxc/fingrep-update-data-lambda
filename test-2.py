import calendar
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


def calc_revenue(revenues):
    # Remove NaN values to prevent arithmetic errors
    revenues = revenues.dropna().tolist()
    total_revenue = sum(revenues)
    list_size = len(revenues)
    # Returns total revenue even if we have two different revenues with the same value
    if list_size == 2:
        return total_revenue
    for i in range(0, list_size):
        if (total_revenue - revenues[i]) == revenues[i]: # checks if revenue[i] is aggregate position
            return revenues[i]
    return total_revenue # returns total revenue if none of the revenues is aggregate


if __name__ == '__main__':
    period_ending = datetime.strptime('2024-08-15', "%Y-%m-%d")
    prev_month_ending_date = period_ending.replace(day=1) - timedelta(days=1)
    curr_month_ending_date = period_ending.replace(day=calendar.monthrange(period_ending.year, period_ending.month)[1])
    print(prev_month_ending_date.date())
    print(curr_month_ending_date.date())