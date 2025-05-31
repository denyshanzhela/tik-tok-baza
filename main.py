import logging
import requests
from flask import Flask, jsonify
from google.cloud import bigquery

# === Настройка логирования ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# === Константы ===
ACCESS_TOKEN = '0ce327e83313303aceccfc156e9fed49d3f8ff7a'
ADVERTISER_ID = '7499963290844545040'
PROJECT_ID = 'disco-bedrock-428721-f8'
DATASET_ID = 'tik_tok'

# === Получение и загрузка метаданных ===

def fetch_and_upload_ads():
    logger.info("🚀 Загружаем ads meta")
    url = "https://business-api.tiktok.com/open_api/v1.3/ad/get/"
    headers = {"Access-Token": ACCESS_TOKEN}
    params = {
        "advertiser_id": ADVERTISER_ID,
        "page_size": 1000
    }

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if data.get("code") != 0:
            raise ValueError(f"Ошибка API: {data.get('message')}")

        ads = data.get("data", {}).get("list", [])
        rows = [{
            "ad_id": ad.get("ad_id"),
            "ad_name": ad.get("ad_name"),
            "adgroup_id": ad.get("adgroup_id"),
            "status": ad.get("status")
        } for ad in ads]

        client = bigquery.Client(project=PROJECT_ID)
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.tiktok_ads_meta"
        errors = client.insert_rows_json(table_ref, rows)
        if errors:
            logger.error(f"❌ Ошибки загрузки ads: {errors}")
            return False

        logger.info(f"✅ Загружено объявлений: {len(rows)}")
        return True

    except Exception as e:
        logger.error(f"🔥 Ошибка загрузки ads: {str(e)}")
        return False

def fetch_and_upload_adgroups():
    logger.info("🚀 Загружаем adgroups meta")
    url = "https://business-api.tiktok.com/open_api/v1.3/adgroup/get/"
    headers = {"Access-Token": ACCESS_TOKEN}
    params = {
        "advertiser_id": ADVERTISER_ID,
        "page_size": 1000
    }

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if data.get("code") != 0:
            raise ValueError(f"Ошибка API: {data.get('message')}")

        adgroups = data.get("data", {}).get("list", [])
        rows = [{
            "adgroup_id": ag.get("adgroup_id"),
            "adgroup_name": ag.get("adgroup_name"),
            "campaign_id": ag.get("campaign_id"),
            "status": ag.get("status")
        } for ag in adgroups]

        client = bigquery.Client(project=PROJECT_ID)
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.tiktok_adgroups_meta"
        errors = client.insert_rows_json(table_ref, rows)
        if errors:
            logger.error(f"❌ Ошибки загрузки adgroups: {errors}")
            return False

        logger.info(f"✅ Загружено adgroups: {len(rows)}")
        return True

    except Exception as e:
        logger.error(f"🔥 Ошибка загрузки adgroups: {str(e)}")
        return False

def fetch_and_upload_campaigns():
    logger.info("🚀 Загружаем campaigns meta")
    url = "https://business-api.tiktok.com/open_api/v1.3/campaign/get/"
    headers = {"Access-Token": ACCESS_TOKEN}
    params = {
        "advertiser_id": ADVERTISER_ID,
        "page_size": 1000
    }

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if data.get("code") != 0:
            raise ValueError(f"Ошибка API: {data.get('message')}")

        campaigns = data.get("data", {}).get("list", [])
        rows = [{
            "campaign_id": c.get("campaign_id"),
            "campaign_name": c.get("campaign_name"),
            "objective_type": c.get("objective_type"),
            "status": c.get("status")
        } for c in campaigns]

        client = bigquery.Client(project=PROJECT_ID)
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.tiktok_campaigns_meta"
        errors = client.insert_rows_json(table_ref, rows)
        if errors:
            logger.error(f"❌ Ошибки загрузки campaigns: {errors}")
            return False

        logger.info(f"✅ Загружено campaigns: {len(rows)}")
        return True

    except Exception as e:
        logger.error(f"🔥 Ошибка загрузки campaigns: {str(e)}")
        return False

# === Flask Routes ===

@app.route("/")
def health():
    return jsonify({"status": "healthy"}), 200

@app.route("/update_meta", methods=["POST"])
def update_all_meta():
    results = {
        "ads": fetch_and_upload_ads(),
        "adgroups": fetch_and_upload_adgroups(),
        "campaigns": fetch_and_upload_campaigns()
    }
    success = all(results.values())
    return jsonify({"status": "success" if success else "error", "details": results}), (200 if success else 500)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
