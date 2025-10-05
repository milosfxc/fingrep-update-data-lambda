table_ids = {
    'shares': {'id'},
    'shares_info': {'share_id'},
    'd_timeframe': {'share_id', 'date'},
    'income_statement': {'share_id', 'date', 'report_type'},
    'cash_flow_statement': {'share_id', 'date', 'report_type'},
    'balance_sheet': {'share_id', 'date', 'report_type'},
    'ratios': {'share_id', 'date', 'report_type'}
}
# Ordered for proper insertion into database
csv_files =  [
    # 'db_data/d_timeframe.bulk.csv',
    'db_data/shares.csv',
    'db_data/shares_info.csv',
    'db_data/delete_ids.csv',
    'db_data/d_timeframe.csv',
    'db_data/balance_sheet.csv',
    'db_data/income_statement.csv',
    'db_data/cash_flow_statement.csv',
    'db_data/ratios.csv'
    ]
