from locale import currency

import db_ops
import edgar
import fingrep_service
from db_ops import delete_fillings_older_than_four_days

# SEC EDGAR latest filings URL
url = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent'

if __name__ == "__main__":
    # Update fundamentals
    # data = edgar.get_latest_fillings()
    # if data:
    #     db_ops.insert_latest_fillings(data)
    # ciks = db_ops.get_fillings_older_than_four_days()
    # if ciks:
    #     for cik in ciks:
    #         result = db_ops.get_id_and_ticker_and_currency_id_by_cik(cik)
    #         if result:
    #             share_id = result.get('share_id')
    #             currency_id = result.get('currency_id')
    #             ticker = result.get('ticker')
    #             if share_id and currency_id and ticker:
    #                 fingrep_service.get_and_insert_fundamentals(cik=cik, share_id=share_id, currency_id=currency_id,
    #                                                             ticker=ticker, period='A')
    # db_ops.delete_fillings_older_than_four_days()

    #fingrep_service.get_and_insert_fundamentals(cik='0001997652', share_id=382, currency_id=1, ticker='TBN', period='A')
    #fingrep_service.get_and_insert_fundamentals(cik='0001576873', share_id=382, currency_id=1, ticker='ABAT', period='A')
    #fingrep_service.get_and_insert_fundamentals(cik='0000732026', share_id=382, currency_id=1, ticker='TRT', period='A')
    fingrep_service.get_and_insert_fundamentals(cik='0001512228', share_id=87, currency_id=1, ticker='NB', period='A')