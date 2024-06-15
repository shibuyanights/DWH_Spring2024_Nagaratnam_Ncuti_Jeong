import json
import pandas as pd
import requests
import boto3
from io import StringIO

def lambda_handler(event, context):
    s3_bucket = 'zrhdata'
    master_file_key = 'timetable.csv'
    
    df = get_flight_data()
    
    s3 = boto3.client('s3')
    
    try:
        response = s3.get_object(Bucket=s3_bucket, Key=master_file_key)
        existing_df = pd.read_csv(response['Body'])
        updated_df = pd.concat([existing_df, df], ignore_index=True)
    except s3.exceptions.NoSuchKey:        updated_df = df

    csv_buffer = StringIO()
    updated_df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)  
    s3.put_object(Bucket=s3_bucket, Key=master_file_key, Body=csv_buffer.getvalue())
    
    return {
        'statusCode': 200,
        'body': json.dumps('Flight data successfully updated in S3')
    }

def get_flight_data():
    url = 'https://dxp-fds.flughafen-zuerich.ch/flights'
    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; AWS Lambda)',
        'Accept': 'application/json'
    }
    response = requests.get(url, headers=headers)
    data = response.json()
    df = pd.DataFrame(data)
    return df

if __name__ == '__main__':
    result = lambda_handler(None, None)
    print(result)
