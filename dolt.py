import datetime
import json

import edgar
import pandas as pd
import yfinance as yf
from edgar import get_filings, Company, XBRL
from edgar.xbrl.stitching import XBRLS

import XBRLTagMapper
import config
import db_ops
import edgar_service
import fingrep_service
import fundamentals
import polygon
import utils
import xbrl_utils
from fundamentals import get_period_ending_by_accession_number
from utils import get_utc_date

# Set pandas to display all rows and columns
pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_colwidth', None)  # No truncation in columns
pd.set_option('display.width', 1000)  # To allow the console to use the full width
pd.set_option('display.float_format', '{:,.2f}'.format)
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
    #print(edgar.get_by_accession_number('0001690820-25-000074').obj().financials.xbrl_data.instance.query_facts(schema='us-gaap',concept='us-gaap:NetIncomeLoss')[['value', 'context_id','units', 'decimals', 'start_date', 'end_date', 'duration']])
    #print(edgar.get_by_accession_number('0001690820-25-000074').obj().financials.xbrl_data.instance.contexts)
    #print(edgar.get_by_accession_number('0001690820-25-000074').obj().financials.xbrl_data.instance.contexts.get('c-9', {}))
    #print(edgar.get_by_accession_number('0001690820-25-000074').obj().financials.xbrl_data.instance.contexts.get('c-1', {}))
    #print(edgar.get_by_accession_number('0001690820-25-000074').obj().financials.xbrl_data.instance.contexts.get('c-29', {}))
    #print(edgar.get_by_accession_number('0001690820-25-000074').obj().financials.xbrl_data.instance.contexts.get('c-30', {}))
    #print(edgar.get_by_accession_number('0001690820-25-000074').obj().financials.xbrl_data.instance.units)
    #df = edgar.get_by_accession_number('0001193125-24-167462').obj().financials.get_income_statement().get_dataframe()
    #first_and_last_columns = df.iloc[:, [0, -1]]  # Selects first and last columns
    #print(first_and_last_columns.to_csv())
    #x = edgar.get_by_accession_number('0001289877-21-000008 ').obj().financials.xbrl_data.instance.query_facts(value='89177000')['units'].iloc[0]
    #print(edgar_service.get_filing_currency('89177000',x))
    # if db_ops.foreign_keys_cache is None:
    #    db_ops.get_foreign_keys()
    # print(db_ops.foreign_keys_cache['currencies'].get())
    #print(db_ops.foreign_keys_cache['currencies']['USD'])
    # filings = get_filings(form="13F-HR")
    # filing = filings[0]
    # thirteenf = filing.obj()
    # print(thirteenf)
    # complex_query = (xbrl.query()
    #                  .by_statement("IncomeStatement")
    #                  .by_label("Revenue")
    #                  .by_value(lambda x: x > 1_000_000)
    #                  .sort_by('value', ascending=False)
    #                  .limit(10))
    filing = Company('CVNA').latest('10-K')
    df = filing.xbrl().statements.income_statement()#.xbrl().query().by_statement_type('IncomeStatement').by_instant_date().by_date_range('2023-07-01', '2024-06-30').by_dimension(None).by_value(11.86).to_dataframe()
    df = df.to_dataframe()
    df = pd.to_numeric(df[filing.period_of_report], errors='coerce').dropna()

    #print(fundamentals.get_period_start(filing))
    print(fundamentals.get_period_start_and_currency(filing))


    # df = Company('AAL').latest('10-K').xbrl().query(include_dimensions=True).by_dimension(None).by_instant_date('2024-12-31').by_concept("us-gaap:Assets",exact=True).to_dataframe()
    # df_1 = Company('AAL').latest('10-K').xbrl().query(include_dimensions=True).by_dimension(None).by_date_range('2024-01-01', '2024-12-31').by_concept("us-gaap:EarningsPerShareBasic",exact=True).to_dataframe()
    # print(df)
    # print(df_1)
    # df = fundamentals.get_statement(stmt_name='cf',df_stmt=df,acc_standard='us-gaap', report_date='2024-12-31')
    # print(df)
    #print(df[['concept', 'label', 'dimension', '2024-06-30']])

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
    #ticker = yf.Ticker('CHEF')
    #print(ticker.balance_sheet)
    # print(ticker.info.get('x', '').strip())
    #fingrep_service.update_fundamentals()
    #fingrep_service.get_and_insert_fundamentals(455, 'RWL','1378872', '2023-12-31',True,'2024-01-17')
    # df_partial_insert = pd.DataFrame({'cik': [1, 2, 3], 'data': ['A', 'B', 'C']})
    #df_cik = pd.DataFrame({'cik': [2, 3, 4], 'info': ['X', 'Y', 'Z']})
    # df_empty = pd.DataFrame(data=None,columns=['cik','share_id'])
    # df_partial_insert = pd.merge(df_partial_insert, df_empty, how='left', on='cik')
    # print(df_partial_insert)
    #df = fundamentals.get_filings_by_company('TSLA')
    #df = df.head(1)
    #print(df)
    #df = fundamentals.get_filing_details_v2('0000310522-23-000589', 1)
    # Output: {('a', 'b', 'c'): 'your_value'}
    #print(edgar.get_filings(form=['10-K','10-Q'],amendments=False,filing_date='2025-05-19').to_pandas())
    #company = Company('MSFT')
    # filing = company.latest("20-F")
    # # Parse XBRL data
    # xbrl = XBRL.from_filing(filing)
    #
    # # Access statements through the user-friendly API
    # statements = xbrl.statements
    #
    # # Display financial statements
    # balance_sheet = statements.balance_sheet().to_dataframe()
    # print(balance_sheet)
    # filing = edgar.get_by_accession_number('0000006201-25-000023')
    # df = filing.obj().financials.balance_sheet().to_dataframe()
    # print(df.iloc[:,0:3].to_csv())
    # #print(df[['label', 'concept']].to_csv())
    # #print(utils.pg_income_statement_columns.values())
    # attachments = filing.attachments
    # presentation_xml = next((a for a in attachments if a.document.endswith('_pre.xml')), None)
    # if presentation_xml:
    #     xml_map = xbrl_utils.parse_xbrl_to_hierarchy(presentation_xml.text())
    #     x = json.dumps(xml_map)
    #     for st in xml_map.keys():
    #         if 'operations' in st.lower():
    #             print(st)
    #     with open("data/my_file.txt", "w") as f:
    #         f.writelines(x)



        #xbrl_utils.visualize_xbrl_hierarchy(xml_map)
        # xbrls = XBRLS.from_filings(filings)
    # stitched_statements = xbrls.statements
    #
    #
    # # Multi-period statements
    # income_trend = stitched_statements.income_statement()
    # balance_sheet_trend = stitched_statements.balance_sheet()
    # cashflow_trend = stitched_statements.cashflow_statement()
    # print(income_trend.to_dataframe().to_csv())
    # print(utils.pg_income_statement_columns.values())
    #tag_map_1 = xbrl_utils.get_all_children('loc_CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsIncludingDisposalGroupAndDiscontinuedOperationsAbstract', '/home/milos/Downloads/us-gaap-2025/stm/us-gaap-stm-scf-dbo-pre-2025.xml', 'presentationArc')
    # tag_map_2 = xbrl_utils.get_all_children_v2('loc_NetCashProvidedByUsedInInvestingActivitiesAbstract', '/home/milos/Downloads/us-gaap-2025/stm/us-gaap-stm-scf-indir-pre-2025.xml', 'presentationArc')
    # tag_map_3 = xbrl_utils.get_all_children_v2('loc_NetCashProvidedByUsedInInvestingActivitiesAbstract', '/home/milos/Downloads/us-gaap-2025/stm/us-gaap-stm-scf-indira-pre-2025.xml', 'presentationArc')
    # tag_map_add = xbrl_utils.get_all_children_v2('loc_AdditionalCashFlowElementsFinancingActivitiesAbstract', '/home/milos/Downloads/us-gaap-2025/stm/us-gaap-stm-scf-indira-pre-2025.xml', 'presentationArc')
    # tag_map_4 = xbrl_utils.get_all_children_v2('loc_NetCashProvidedByUsedInInvestingActivitiesAbstract', '/home/milos/Downloads/us-gaap-2025/stm/us-gaap-stm-scf-inv-pre-2025.xml', 'presentationArc')
    # tag_map_5 = xbrl_utils.get_all_children_v2('loc_NetCashProvidedByUsedInInvestingActivitiesAbstract', '/home/milos/Downloads/us-gaap-2025/stm/us-gaap-stm-scf-re-pre-2025.xml', 'presentationArc')
    # tag_map_6 = xbrl_utils.get_all_children_v2('loc_NetCashProvidedByUsedInInvestingActivitiesAbstract', '/home/milos/Downloads/us-gaap-2025/stm/us-gaap-stm-scf-sbo-pre-2025.xml', 'presentationArc')
    #print(json.dumps(tag_map_1))
