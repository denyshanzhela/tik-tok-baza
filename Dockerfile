FROM python:3.10-slim

# Установка зависимостей
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc python3-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Сначала копируем только requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Затем копируем остальные файлы
COPY . .

# Проверка установки Flask
RUN python -c "import flask; print(f'Flask version: {flask.__version__}')"

# Порт и команда запуска
ENV PORT=8080
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--timeout", "120", "main:app"]
