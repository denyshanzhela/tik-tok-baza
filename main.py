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
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN', 'ae207063979208834126b148731b75d90eb5482')
ADVERTISER_ID = os.getenv('ADVERTISER_ID', '7499963290844545040')
PROJECT_ID = os.getenv('PROJECT_ID', 'disco-bedrock-428721-f8')
DATASET_ID = os.getenv('DATASET_ID', 'tik_tok')
TABLE_ID = os.getenv('TABLE_ID', 'tiktok_ads_stats')

def get_yesterday():
    today = datetime.date.today()
    return (today - datetime.timedelta(days=1)).isoformat()

def get_ads_stats():
    logger.info("🚀 Функция get_ads_stats запущена")
    url = 'https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/'
    headers = {
        'Access-Token': ACCESS_TOKEN,
        'Content-Type': 'application/json'
    }
    
    yesterday = get_yesterday()
    logger.info(f"🔄 Запрашиваю данные TikTok API за {yesterday}")
    
    payload = {
        "advertiser_id": ADVERTISER_ID,
        "report_type": "BASIC",
        "data_level": "AUCTION_AD",
        "dimensions": ["ad_id"],
        "metrics": ["spend", "impressions", "clicks"],
        "start_date": yesterday,
        "end_date": yesterday,
        "page_size": 1000
    }
    
    try:
        logger.info(f"📤 Отправляю запрос к TikTok API: {payload}")
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        logger.info(f"📥 Получен ответ от TikTok API: {data}")

        if "data" not in data or "list" not in data["data"]:
            logger.error(f"❌ Неожиданная структура ответа: {data}")
            raise ValueError("Неверная структура ответа от API")

        stats = data["data"]["list"]
        logger.info(f"✅ Получено {len(stats)} записей")

        # Добавляем дату отчета
        for row in stats:
            row['report_date'] = yesterday

        return stats

    except Exception as e:
        logger.error(f"❌ Ошибка при получении данных: {str(e)}")
        raise


def upload_to_bigquery(rows):
    if not rows:
        logger.warning("⚠️ Нет данных для загрузки в BigQuery")
        return False
        
    logger.info(f"📊 Начало загрузки {len(rows)} строк в BigQuery")
    
    try:
        client = bigquery.Client()
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        logger.info(f"🔍 Проверяю таблицу {table_ref}")
        table = client.get_table(table_ref)
        logger.info(f"ℹ️ Схема таблицы: {table.schema}")
        
        errors = client.insert_rows_json(table_ref, rows)
        
        if errors:
            logger.error(f"❌ Ошибки при загрузке: {errors}")
            return False
            
        logger.info(f"🎉 Успешно загружено {len(rows)} строк")
        return True
        
    except Exception as e:
        logger.error(f"🔥 Критическая ошибка BigQuery: {str(e)}")
        raise

@app.route('/')
def health_check():
    logger.info("🏥 Проверка здоровья сервиса")
    return jsonify({"status": "healthy"}), 200

@app.route('/run', methods=['POST'])
def run_etl():
    try:
        logger.info("🚀 Запуск ETL-процесса")
        
        stats = get_ads_stats()
        if not stats:
            logger.warning("⚠️ Нет данных для обработки")
            return jsonify({"status": "success", "message": "No data to process"}), 200
            
        success = upload_to_bigquery(stats)
        
        if success:
            logger.info("✨ ETL-процесс успешно завершен")
            return jsonify({
                "status": "success",
                "message": "Data processed successfully",
                "rows_processed": len(stats)
            }), 200
        else:
            logger.error("💥 ETL-процесс завершен с ошибками")
            return jsonify({
                "status": "partial_success",
                "message": "Data processed with some errors"
            }), 207
            
    except Exception as e:
        logger.error(f"💀 Фатальная ошибка в ETL: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
