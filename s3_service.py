import csv
from typing import List, Dict, Any
import io
import boto3
from botocore.exceptions import ClientError


def read_csv(bucket_name: str, file_path: str) -> Dict | None:
    try:
        ans = dict()
        s3_client = boto3.client('s3',region_name='us-east-1')
        response = s3_client.get_object(Bucket=bucket_name, Key=file_path)
        # Convert csv data into dict
        file_content = response['Body'].read().decode('utf-8')
        reader = csv.DictReader(io.StringIO(file_content))
        rows = [row for row in reader]
        # Get data and metadata
        ans['data'] = rows
        ans['table'] = response['Metadata']['table']
        ans['timestamp'] = response['Metadata']['timestamp']
        return ans

    except Exception as e:
        raise Exception(f"Error reading CSV: {str(e)}")


def get_s3_data() -> Dict | None:
    try:
        s3_client = boto3.client('s3',region_name='us-east-1')
        response = s3_client.list_objects_v2(Bucket='fingrep', Prefix='db_data/')
        timestamp = s3_client.get_object(Bucket='fingrep', Key='db_data/trigger.txt')['Metadata']['timestamp']
        ans = dict()
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].endswith('.csv'):
                    csv_data = read_csv('fingrep', obj['Key'])
                    if csv_data['timestamp'] == timestamp:
                        ans[obj['Key']] = csv_data
        return ans
    except Exception as e:
        raise Exception(f"Error reading CSV: {str(e)}")