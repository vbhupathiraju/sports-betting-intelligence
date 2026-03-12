FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY secrets_helper.py .
COPY msk_producer.py .
COPY odds_api_producer/main.py ./odds_api_producer/main.py

CMD ["python", "odds_api_producer/main.py"]
