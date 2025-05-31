from flask import Flask, jsonify
import requests
import datetime
import os
import logging
from google.cloud import bigquery

app = Flask(__name__)

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN', '0ce327e83313303aceccfc156e9fed49d3f8ff7a')
ADVERTISER_ID = os.getenv('ADVERTISER_ID', '7499963290844545040')
PROJECT_ID = os.getenv('PROJECT_ID', 'disco-bedrock-428721-f8')
DATASET_ID = os.getenv('DATASET_ID', 'tik_tok')
TABLE_ID = os.getenv('TABLE_ID', 'tiktok_ads_stats')

def get_yesterday():
    return (datetime.date.today() - datetime.timedelta(days=1)).isoformat()

def get_ads_stats(date_str):
    logger.info(f"🚀 Получаем данные TikTok за {date_str}")
    url = 'https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/'

    headers = {
        'Access-Token': ACCESS_TOKEN
    }

    params = {
        "advertiser_id": ADVERTISER_ID,
        "report_type": "BASIC",
        "data_level": "AUCTION_AD",
        "dimensions": '["ad_id"]',
        "metrics": '["spend", "impressions", "clicks"]',
        "start_date": date_str,
        "end_date": date_str,
        "page_size": 1000
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if data.get("code") != 0:
            raise ValueError(f"API вернул ошибку: {data.get('message')}")

        stats = data.get("data", {}).get("list", [])
        logger.info(f"✅ Получено {len(stats)} строк")

        result = []
        for row in stats:
            dims = row.get("dimensions", {})
            mets = row.get("metrics", {})

            impressions = int(mets.get("impressions", 0))
            if impressions == 0:
                continue  # фильтрация по impressions > 0

            result.append({
                'ad_id': dims.get("ad_id"),
                'spend': float(mets.get("spend", 0)),
                'impressions': impressions,
                'clicks': int(mets.get("clicks", 0)),
                'date': date_str
            })

        return result

    except Exception as e:
        logger.error(f"❌ Ошибка при получении данных: {str(e)}")
        return []

def upload_to_bigquery(rows):
    if not rows:
        logger.warning("⚠️ Нет данных для загрузки")
        return False

    try:
        client = bigquery.Client()
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

        logger.info(f"📊 Загрузка в {table_ref}")
        errors = client.insert_rows_json(table_ref, rows)

        if errors:
            logger.error(f"❌ Ошибки при загрузке: {errors}")
            return False

        logger.info(f"✅ Загружено {len(rows)} строк")
        return True

    except Exception as e:
        logger.error(f"🔥 Ошибка при загрузке в BigQuery: {str(e)}")
        return False

@app.route('/')
def health():
    return jsonify({"status": "healthy"}), 200

@app.route('/run', methods=['POST'])
def run_etl():
    date_str = get_yesterday()
    stats = get_ads_stats(date_str)

    if not stats:
        logger.warning("⚠️ Нет данных для загрузки")
        return jsonify({"status": "success", "message": "Нет данных"}), 200

    success = upload_to_bigquery(stats)
    if success:
        return jsonify({"status": "success", "rows_uploaded": len(stats)}), 200
    else:
        return jsonify({"status": "error", "message": "Ошибка при загрузке"}), 500
