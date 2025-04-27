import datetime

import edgar
import pandas as pd
import yfinance as yf
from edgar import get_filings, Company

import config
import db_ops
import edgar_service
import fingrep_service
import polygon
from fundamentals import get_period_ending_by_accession_number
from utils import get_utc_date

# Set pandas to display all rows and columns
pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_colwidth', None)  # No truncation in columns
pd.set_option('display.width', 1000)  # To allow the console to use the full width
#pd.set_option("display.expand_frame_repr", False)  # Prevent line wrapping between columns
#pd.set_option("display.colheader_justify", "left") # Left-align headers
if __name__ == '__main__':
    #print(db_ops.get_dolt_statement('ADC','income_statement', False))
    #print(fingrep_service.get_and_insert_fundamentals(799, 'NWTG', '1934245'))
    #print(db_ops.get_id_and_cik())
    #fingrep_service.update_fundamentals()
    #print(db_ops.get_ids_by_by_cik(['1690080', '1650648']))
    #df_full_insert = db_ops.get_filings_older_than_four_days_and_before_last_sunday()
    # if not df_full_insert.empty:
    #     df_full_insert['period_ending'] = df_full_insert.apply(lambda row: get_period_ending_by_accession_number(accession_number=row['accession_number']),axis=1)
    #     equal_dates_df = df_full_insert[
    #         df_full_insert['period_ending'].astype(str) == df_full_insert['filing_date'].astype(str)]
    #     print(equal_dates_df)

    #print(db_ops.get_dolt_statement(ticker='NWTG',table_name='income_statement',latest=False))
    #filings = get_filings(form=['10-K', '10-Q'], filing_date='2025-04-04', amendments=False)
    #print(filings.to_pandas())
    #x = edgar.get_by_accession_number('0001289877-21-000008 ').obj().financials.xbrl_data.instance.dimensions#query_facts(concept="us-gaap:Revenues",dimensions={""})

    #x = edgar.get_by_accession_number('0001289877-21-000008 ')#.obj().financials.xbrl_data.instance.query_facts(value='89177000')['units'].iloc[0]
    #print(edgar_service.get_filing_currency('89177000',x))
    # if db_ops.foreign_keys_cache is None:
    #    db_ops.get_foreign_keys()
    # print(db_ops.foreign_keys_cache['currencies'].get())
    #print(db_ops.foreign_keys_cache['currencies']['USD'])
    # filings = get_filings(form="13F-HR")
    # filing = filings[0]
    # thirteenf = filing.obj()
    # print(thirteenf)
    #c = Company('ALVO').latest('20-F')#.obj().financials.get_balance_sheet().get_dataframe()
    #print(c)
    #print(edgar_service.get_filing_currency('30737400000', c))
    #print(get_filings(filing_date='2024-05-28',form='6-K'))
    # df = fingrep_service.get_grouped_daily_bars(get_utc_date(days=config.DAYS))
    # df_prev = fingrep_service.get_prev_grouped_daily_bars()
    # # Find tickers that aren't in df_prev
    #
    # new_tickers = df[~df['T'].isin(df_prev['T'])]
    # df = fingrep_service.get_grouped_daily_bars(date_str=get_utc_date(days=config.DAYS))
    # df_prev = fingrep_service.get_prev_grouped_daily_bars()
    # df_diff = df[~df['T'].isin(df_prev['T'])]
    # df_all = pd.DataFrame(fingrep_service.get_all_tickers(date_str=get_utc_date(days=config.DAYS)))# join this dataframe with df_diff, join by column t.
    # df_all.rename(columns={'ticker':'T'}, inplace=True)
    # merged_df = pd.merge(
    #     df_diff,
    #     df_all,
    #     on='T',  # Join key (ticker column)
    #     how='left'  # Keep all rows from df_diff, add matching data from df_all
    # )
    # print(merged_df[['T', 'composite_figi', 'cik', 'type']])
    # ticker = yf.Ticker('VGI')
    # print(ticker.info.get('x', '').strip())
    #fingrep_service.get_and_insert_fundamentals(610, 'VIOT','1742770')
    df_partial_insert = pd.DataFrame({'cik': [1, 2, 3], 'data': ['A', 'B', 'C']})
    df_cik = pd.DataFrame({'cik': [2, 3, 4], 'info': ['X', 'Y', 'Z']})
    df_empty = pd.DataFrame(data=None,columns=['cik','share_id'])
    df_partial_insert = pd.merge(df_partial_insert, df_empty, how='left', on='cik')
    print(df_partial_insert)
