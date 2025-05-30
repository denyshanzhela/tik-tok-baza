from flask import Flask, jsonify
import requests
import json
import datetime
from google.cloud import bigquery

app = Flask(__name__)

ACCESS_TOKEN = 'ae207063979208834126b148731b75d90eb5482'
ADVERTISER_ID = '7499963290844545040'
PROJECT_ID = 'disco-bedrock-428721-f8'
DATASET_ID = 'tik_tok'
TABLE_ID = 'tiktok_ads_stats'

def get_yesterday():
    today = datetime.date.today()
    return (today - datetime.timedelta(days=1)).isoformat()

def get_ads_stats():
    url = 'https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/'
    headers = {
        'Access-Token': ACCESS_TOKEN,
        'Content-Type': 'application/json'
    }
    payload = {
        "advertiser_id": ADVERTISER_ID,
        "report_type": "BASIC",
        "data_level": "AUCTION_AD",
        "dimensions": ["ad_id"],
        "metrics": ["spend", "impressions", "clicks"],
        "start_date": get_yesterday(),
        "end_date": get_yesterday(),
        "page_size": 1000
    }
    response = requests.post(url, headers=headers, json=payload)
    data = response.json()
    if "data" in data and "list" in data["data"]:
        return data["data"]["list"]
    else:
        raise Exception(f"Error in response: {data}")

def upload_to_bigquery(rows):
    client = bigquery.Client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        raise Exception(f"BigQuery errors: {errors}")
    return True

@app.route('/')
def run_etl():
    try:
        stats = get_ads_stats()
        upload_to_bigquery(stats)
        return jsonify({"status": "success", "message": "Data processed successfully"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
