import datetime

from edgar import get_filings, set_identity, get_by_accession_number
from datetime import timedelta

from fmpsdk import income_statement

import db_ops
import pandas as pd

import edgar_service
import fingrep_service
import utils
# Set pandas to display all rows and columns
pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', None)  # To allow the console to use the full width

set_identity('milosfxc@gmail.com')
def update_latest_filings():
    # Get max date from latest filings
    current_date = datetime.date.today()
    latest_filings = db_ops.get_latest_filings_by_max_filing_date()
    df = None
    if latest_filings:
        df = pd.DataFrame(latest_filings)
        print(df)
        max_filing_date = pd.to_datetime(df.loc[0, 'filing_date']).date()
    else:
        max_filing_date = current_date
    delta = (current_date - max_filing_date).days + 1
    # Get all filings from latest filing date to current date
    df_edgar = None
    for i in range(0, delta):
        fd = max_filing_date + timedelta(days=i)
        if fd.weekday() not in [5, 6]:
            filings = get_filings(form=['10-K', '10-Q'], filing_date=fd.strftime('%Y-%m-%d'), amendments=False)
            if filings:
                df_edgar = pd.concat([df_edgar, filings.to_pandas()], ignore_index=True) if df_edgar is not None else filings.to_pandas()
    if df_edgar is not None and not df_edgar.empty:
        # Replace values in form column, 10-A = 1, 10-Q = 2
        df_edgar['report_type_id'] = df_edgar['form'].replace(utils.form_report_type_id)
        # Drop unnecessary columns
        #df_edgar.drop(columns=['company', 'form'], inplace=True)
        df_edgar['partially_inserted'] = False
        df_edgar['inserted'] = False
        # Remove already partially inserted rows
        if df is not None and not df.empty:
            partially_inserted_filings = df[df['partially_inserted'] == True]['accession_number'].tolist()
            df_edgar = df_edgar[~df_edgar['accession_number'].isin(partially_inserted_filings)]
        # Get income stmt positions
        df_edgar[['revenue', 'eps', 'net_income', 'period_ending', 'partially_inserted']] = df_edgar.head(10).apply(update_income_positions, axis=1)


def update_income_positions(row):
    accession_number = row['accession_number']
    filing = get_by_accession_number(accession_number=accession_number)
    if filing is None:
        print('filing is None:')
        return pd.Series([None, None, None, None, False])
    # Period ending
    period_ending = filing.period_of_report
    if period_ending is None:
        print('period_ending is None:')
        return pd.Series([None, None, None, None, False])
    # Retrieve the income statement
    try:
        # Ensure filing object is not None
        filing_obj = filing.obj() if filing else None
        if filing_obj is None or filing_obj.financials is None:
            print("filing.obj() is None or has no financials")
            return pd.Series([None, None, None, None, False])
        inc_stmt = filing_obj.financials.get_income_statement()
        df_filing = inc_stmt.get_dataframe() if inc_stmt else None
        if df_filing is None or df_filing.empty:
            print("df_filing is None or df_filing.empy:")
            return pd.Series([None, None, None, None, False])
    except Exception as e:
        print(e.with_traceback())
        print('except Exception as e:')
        return pd.Series([None, None, None, None, False])
    # Convert string to numeric
    df_filing.iloc[:, 0] = pd.to_numeric(df_filing.iloc[:, 0], errors='coerce')
    # Dataframe that contains revenue word in concept column rows
    df_revenue = df_filing[df_filing['concept'].str.contains('revenue', case=False, na=False)]
    # Retrieve the max value
    max_val = df_revenue.iloc[:,0].max()
    # Filter rows where the first column matches max_val
    df_max = df_revenue[df_revenue.iloc[:, 0] == max_val]
    # Ensure df_max is not empty before accessing values
    if df_max.empty:
        print("df_max is empty, no matching max value found.")
        return pd.Series([None, None, None, None, False])
    # Extract concept with max value safely
    max_val_concept = df_max['concept'].values[0]
    # Filter rows with the same concept as max_val_concept
    df_revenue = df_filing[df_filing['concept'] == max_val_concept]
    # Calc revenue safely to prevent IndexError
    revenue = calc_revenue(df_revenue)
    if revenue: revenue = revenue * 10000
    # Calc eps safely to prevent IndexError
    df_eps = df_filing[df_filing['concept'] == 'us-gaap_EarningsPerShareBasic']
    eps = df_eps.iloc[0, 0] * 10000 if not df_eps.empty else None
    # Calc net income
    df_net_income = df_filing[df_filing['concept'] == 'us-gaap_NetIncomeLoss']
    net_income = df_net_income.iloc[0, 0] * 10000 if not df_net_income.empty else None
    # Returns revenue, eps, net income, period ending and is_insertable
    return pd.Series([revenue, eps, net_income, period_ending, True])

def calc_revenue(revenues):
    # Remove NaN values to prevent arithmetic errors
    revenues = revenues.iloc[:,0].dropna().tolist()
    total_revenue = sum(revenues)
    list_size = len(revenues)
    # Returns total revenue even if we have two different revenues with the same value
    if list_size == 2:
        return total_revenue
    for i in range(0, list_size):
        if (total_revenue - revenues[i]) == revenues[i]: # checks if revenue[i] is aggregate position
            return revenues[i]
    return total_revenue # returns total revenue if none of the revenues is aggregate

if __name__ == "__main__":
    from datetime import datetime, timedelta
    import calendar


    def get_bounds(date_str):
        given_date = datetime.strptime(date_str, "%Y-%m-%d")

        # Lower bound: Last date of the previous month
        first_day_of_current_month = given_date.replace(day=1)
        last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)

        # Upper bound: Last date of the current month
        last_day_of_current_month = calendar.monthrange(given_date.year, given_date.month)[1]
        upper_bound = given_date.replace(day=last_day_of_current_month)

        return last_day_of_previous_month.date(), upper_bound.date()


    # Example usage
    date_str = "2024-02-04"
    lower, upper = get_bounds(date_str)
    print("Lower bound:", lower)
    print("Upper bound:", upper)

