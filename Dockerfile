FROM python:3.10-slim

# Установка зависимостей для google-cloud-bigquery
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Используем gunicorn для production
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 120 main:app
