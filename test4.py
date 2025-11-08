

import utils
if __name__ == '__main__':
    # ticker_id_map = {'MSFT':1, 'AAPL':2}
    # tickers = ['MSFT', 'AAPL']
    # short_volume = polygon_service.batch_requests(tickers=tickers,
    #                                               request_function=polygon_service.request_short_volume,
    #                                               batch_size=300,
    #                                               date = utils.get_utc_date(config.days),
    #                                               date_operator='')
    # print(short_volume)
    # def batch_requests(tickers:list, request_function: callable,batch_size:int, date:str, date_operator:str = '') -> list[dict] | None:
    from pprint import pprint

    sv_list = [
        {'f_volume': 378605, 'date': '2024-02-06', 'short_volume_ratio': 54.76, 'non_exempt_volume': 207120,
         'short_volume': 207342, 'exempt_volume': 222, 'share_id': 4},
        {'f_volume': 799432, 'date': '2024-02-07', 'short_volume_ratio': 47.93, 'non_exempt_volume': 374120,
         'short_volume': 383185, 'exempt_volume': 9065, 'share_id': 4}
    ]

    si_list = [
        {'avg_f_volume': 947259, 'short_interest_ratio': 2.2, 'short_interest': 2081363, 'date': '2023-02-07',
         'share_id': 4},
        {'avg_f_volume': 1490008, 'short_interest_ratio': 1.48, 'short_interest': 2200773, 'date': '2023-11-30',
         'share_id': 4}
    ]