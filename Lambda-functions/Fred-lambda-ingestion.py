import json
import boto3
import requests
import os
import pandas as pd
from datetime import datetime, timedelta

s3 = boto3.client('s3')
BUCKET_NAME = os.environ["BUCKET"]
STATE_KEY = "status/fred_state.json"
API_KEY = os.environ["API_KEY"]

# Your specific Macro series
SERIES = {
    'VIX': 'VIXCLS', 
    'Brent_Oil': 'DCOILBRENTEU', 
    'US10Y_Yield': 'DGS10'
}

def lambda_handler(event, context):
    today = datetime.now().date()
    
    # 1. 3-Day Lookback Logic
    try:
        res = s3.get_object(Bucket=BUCKET_NAME, Key=STATE_KEY)
        last_run_str = json.loads(res['Body'].read())['to_date']
        from_date = datetime.strptime(last_run_str, '%Y-%m-%d').date() - timedelta(days=3)
    except:
        from_date = today - timedelta(days=60)
        
    # 2. Build the CSV Header as a string
    csv_content = "Date,Value,Indicator\n"

    rows_written = 0
    for name, s_id in SERIES.items():
        url = f"https://api.stlouisfed.org/fred/series/observations?series_id={s_id}&api_key={API_KEY}&file_type=json&observation_start={from_date}&observation_end={today}"
        
        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code == 200:
                obs = resp.json().get('observations', [])
                for o in obs:
                    if o['value'] != '.':
                        # Append rows directly to the string
                        csv_content += f"{o['date']},{o['value']},{name}\n"
                        rows_written += 1
        except Exception as e:
            print(f"Error fetching {name}: {e}")

    # 3. Save String Data to S3
    if rows_written > 0:
        s3.put_object(
            Bucket=BUCKET_NAME, 
            Key=f"fred/fred_{today}.csv", 
            Body=csv_content
        )
    
    # 4. Update State File
    s3.put_object(
        Bucket=BUCKET_NAME, 
        Key=STATE_KEY, 
        Body=json.dumps({"to_date": str(today)})
    )
    
    return {"status": "success", "rows": rows_written}
