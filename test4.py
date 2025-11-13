import aws_service
import config
import db_ops
import fingrep_service
import fundamentals_service
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
    for i in range(1,15):
        aws_service.add_share_id(i, 'new')
    for i in range(17,19):
        aws_service.add_share_id(i,'new')
    aws_service.update_s3_bucket()