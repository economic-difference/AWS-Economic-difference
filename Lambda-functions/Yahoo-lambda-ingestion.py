import json
import boto3
import os
import requests
from datetime import datetime, timedelta

s3 = boto3.client('s3')
BUCKET_NAME = os.environ["BUCKET"]
STATE_KEY = "status/yahoo_state.json"

TICKERS = {
    'China': '000001.SS', 'India': '^NSEI', 
    'Japan': '^N225', 'Singapore': '^STI', 'HongKong': '^HSI'
}

def lambda_handler(event, context):
    today = datetime.utcnow().date()
    
    # 1. 3-Day Lookback Logic
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=STATE_KEY)
        last_run_str = json.loads(response['Body'].read())['to_date']
        # Look back 3 days from the last successful run
        from_date = datetime.strptime(last_run_str, '%Y-%m-%d').date() - timedelta(days=3)
    except:
        # Default for first run
        from_date = today - timedelta(days=60)
    
    start_ts = int(datetime(from_date.year, from_date.month, from_date.day).timestamp())
    end_ts = int(datetime.utcnow().timestamp())
    
    # 2. Build CSV as a string (No StringIO needed)
    csv_content = "Date,country,Close\n"
    rows_written = 0

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    for country, symbol in TICKERS.items():
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?period1={start_ts}&period2={end_ts}&interval=1d"
        try:
            resp = requests.get(url, headers=headers, timeout=15).json()
            result = resp.get("chart", {}).get("result", [None])[0]
            
            if result and "timestamp" in result:
                timestamps = result["timestamp"]
                closes = result["indicators"]["quote"][0]["close"]
                
                for ts, close in zip(timestamps, closes):
                    if close is not None:
                        date_str = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d')
                        # Append row directly to string
                        csv_content += f"{date_str},{country},{close}\n"
                        rows_written += 1
        except Exception as e:
            print(f"Error fetching {country}: {e}")

    # 3. Save Data to Raw Folder
    if rows_written > 0:
        s3.put_object(
            Bucket=BUCKET_NAME, 
            Key=f"yahoo/yahoo_{today}.csv", 
            Body=csv_content
        )
    
    # 4. ALWAYS update state file to maintain the 'status' folder
    s3.put_object(
        Bucket=BUCKET_NAME, 
        Key=STATE_KEY, 
        Body=json.dumps({"to_date": str(today)})
    )
    
    return {"status": "success", "rows": rows_written, "start_date": str(from_date)}