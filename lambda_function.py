import db_ops
import s3_service
from utils import table_ids, csv_files


def lambda_handler(event, context):
    data = s3_service.get_s3_data()
    for k in csv_files:
        if k in data:
            v = data[k]
            db_ops.upsert_from_csv_data_smart(v['data'], v['table'], table_ids[v['table']])
    return {
        'statusCode': 200,
        'body': 'Object list printed in logs'
    }