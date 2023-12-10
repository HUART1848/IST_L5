import datetime
import boto3
import json
import requests

BUCKET="ist-meteo-grh-ferchichi-huart"
KEY="current/VQHA80.csv"

CSV_URL = "https://data.geo.admin.ch/ch.meteoschweiz.messwerte-aktuell/VQHA80.csv"

def fetch_csv(url:str=CSV_URL) -> bytes:
    return requests.get(url).content

def upload_to_s3(csv_data:bytes, bucket_name:str, key:str):
    s3 = boto3.resource("s3")
    obj = s3.Object(bucket_name, key)
    obj.put(Body=csv_data)

def timestamp_key(key:str) -> str:
    now = datetime.datetime.utcnow().isoformat()
    key = os.path.splitext(key)[0]
    return f"{key}-{now}.csv"
    
def main():
    csv_data = fetch_csv()
    key = timestamp_key(KEY)
    upload_to_s3(csv_data, BUCKET, key)

def lambda_handler(event, context):
    main()
    return {
        'statusCode': 200,
        'body': json.dumps('')
    }
