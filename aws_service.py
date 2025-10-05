import csv
import io
import os
from datetime import timezone, datetime
from typing import List, Dict, Any
import boto3
from botocore.exceptions import ClientError
import config
import db_ops
import utils
from config import logger, aws_logger


share_ids = {'new': set(), 'split': set(), 'fundamentals': set()}

def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_FINGREP_DB_DATA_KEY'),
        aws_secret_access_key=os.getenv('AWS_FINGREP_DB_DATA_SECRET')
    )


def add_share_id(share_id, key):
    global share_ids
    if share_id is not None and isinstance(share_id, int):
        share_ids[key].add(share_id)
    else:
        logger.error(f"share_id returned from new ticker insertion was not integer: {share_id}")


def update_s3_bucket():
    # Get data for S3
    new_ids = list(new_ids) if (new_ids := share_ids['new']) else None
    fund_ids = list(fund_ids) if (fund_ids := share_ids['new'].union(share_ids['fundamentals'])) else None
    data = {'d_timeframe.bulk': db_ops.query_data_as_csv('d_timeframe', {'date': utils.get_utc_date(config.days)}),
            'shares': db_ops.query_data_as_csv('shares', {'id': new_ids}) if new_ids else None,
            'shares_info': db_ops.query_data_as_csv('shares_info', {'share_id': new_ids}) if new_ids else None,
            'd_timeframe': db_ops.query_data_as_csv('d_timeframe', {'share_id': list(ids)})  if (ids := share_ids['new'].union(share_ids['split'])) else None,
            'income_statement': db_ops.query_data_as_csv('income_statement', {'share_id': fund_ids}) if fund_ids else None,
            'cash_flow_statement': db_ops.query_data_as_csv('cash_flow_statement', {'share_id': fund_ids}) if fund_ids else None,
            'balance_sheet': db_ops.query_data_as_csv('balance_sheet', {'share_id': fund_ids}) if fund_ids else None,
            'ratios': db_ops.query_data_as_csv('ratios', {'share_id':fund_ids}) if fund_ids else None,
            'delete_ids': 'id\n' + '\n'.join(str(i) for i in share_ids['split']) if share_ids['split'] else None
            }
    # Upload data to S3
    try:
        s3_client = get_s3_client()
        upload_results = {}
        successful_uploads = 0
        total_files = len(data)

        aws_logger.info(f"📤 Starting S3 upload of {total_files} files...")
        utc_datetime = datetime.now(timezone.utc)
        # Upload each CSV data to S3
        for filename, csv_data in data.items():
            if csv_data is not None and csv_data.strip():
                try:
                    s3_key = f"db_data/{filename}.csv"
                    # Upload to S3
                    s3_client.put_object(
                        Bucket='fingrep',
                        Key=s3_key,
                        Body=csv_data.encode('utf-8'),
                        ContentType='text/csv',
                        Metadata={
                            'timestamp': utc_datetime.isoformat(),
                            'table': filename.split('.')[0] if '.' in filename else filename
                        }
                    )

                    # Calculate stats
                    lines = csv_data.strip().split('\n')
                    row_count = len(lines) - 1  # Exclude header
                    file_size = len(csv_data.encode('utf-8'))

                    upload_results[filename] = {
                        'status': 'success',
                        's3_path': f"s3://fg/{s3_key}",
                        'size_bytes': file_size,
                        'row_count': row_count,
                        'upload_time': utils.get_utc_date(0)
                    }

                    successful_uploads += 1
                    aws_logger.info(f"✅ {filename}: {row_count} rows, {file_size} bytes")

                except ClientError as e:
                    upload_results[filename] = {
                        'status': 'error',
                        'error': str(e),
                        's3_path': f"s3://fingrep/db_data/{filename}.csv"
                    }
                    aws_logger.error(f"❌ {filename}: Upload failed - {e}")

            else:
                upload_results[filename] = {
                    'status': 'skipped',
                    'reason': 'No data or empty result'
                }
                aws_logger.warning(f"⚠️  {filename}: No data to upload")

        # Summary
        aws_logger.info(f"\n📊 Upload Summary:")
        aws_logger.info(f"   Successful: {successful_uploads}/{total_files}")
        aws_logger.info(f"   Failed: {len([r for r in upload_results.values() if r['status'] == 'error'])}")
        aws_logger.info(f"   Skipped: {len([r for r in upload_results.values() if r['status'] == 'skipped'])}")
        # Trigger AWS Lambda
        if successful_uploads > 1 and successful_uploads == total_files:
            s3_client.put_object(
                Bucket='fingrep',
                Key='db_data/trigger.txt',
                ContentType='text',
                Metadata={
                    'timestamp': utc_datetime.isoformat()
                }
            )
        else:
            aws_logger.error(f'Lambda not triggered for date {utc_datetime}.')

    except ClientError as e:
        aws_logger.error(f"Error listing files: {e}")


def read_csv_from_s3(bucket_name: str, s3_key: str) -> List[Dict[str, Any]]:
    try:
        # Initialize S3 client with your environment variables
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_DB_DATA_KEY'),
            aws_secret_access_key=os.getenv('AWS_DB_DATA_SECRET'),
        )

        # Get the CSV file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        csv_content = response['Body'].read().decode('utf-8')

        # Read CSV from memory
        reader = csv.DictReader(io.StringIO(csv_content))
        rows = [row for row in reader]

        return rows

    except Exception as e:
        raise Exception(f"Error reading CSV from S3: {e}")


def read_csv_from_file(file_path: str) -> List[Dict[str, Any]]:
    try:
        with open(file_path, 'r', newline='') as file:
            reader = csv.DictReader(file)
            rows = [row for row in reader]

        return rows

    except Exception as e:
        raise
