import requests
import boto3
import json
import os
from datetime import datetime

s3 = boto3.client("s3")
BUCKET_NAME = os.environ["BUCKET"]
STATE_KEY = "status/wb_state.json"

COUNTRIES = {
    "CHN": "China",
    "JPN": "Japan",
    "HKG": "Hong Kong",
    "SGP": "Singapore",
    "IND": "India"
}

INDICATORS = {
    "FR.INR.RINR": "Interest Rate (%)",
    "NY.GDP.MKTP.KD.ZG": "GDP Growth (%)",
    "SP.POP.GROW": "Population Growth (%)",
    "GC.DOD.TOTL.GD.ZS": "Government Debt (% of GDP)",
    "SL.UEM.TOTL.ZS": "Unemployment Rate (%)"
}

def lambda_handler(event, context):
    today = datetime.utcnow().date()
    start_year = today.year - 15
    end_year = today.year - 1

    csv_content = "Year,Country,Indicator,Value\n"
    rows_written = 0

    for ind_code, ind_name in INDICATORS.items():
        country_list = ";".join(COUNTRIES.keys())

        url = (
            f"https://api.worldbank.org/v2/"
            f"country/{country_list}/indicator/{ind_code}"
            f"?format=json&per_page=2000&date={start_year}:{end_year}"
        )

        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        data = resp.json()

        if len(data) < 2 or not data[1]:
            continue

        for item in data[1]:
            value = item.get("value")
            if value is None:
                continue

            year = item["date"]
            country = item["country"]["value"].replace(",", "")

            csv_content += f"{year},{country},{ind_name},{value}\n"
            rows_written += 1

    if rows_written > 0:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f"worldbank/wb_{today}.csv",
            Body=csv_content
        )

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=STATE_KEY,
        Body=json.dumps({"to_date": str(today)})
    )

    return {
        "status": "success",
        "rows_written": rows_written
    }