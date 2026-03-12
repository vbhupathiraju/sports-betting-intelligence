FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY secrets_helper.py .
COPY msk_producer.py .
COPY kalshi_producer/main.py ./kalshi_producer/main.py

CMD ["python", "kalshi_producer/main.py"]
