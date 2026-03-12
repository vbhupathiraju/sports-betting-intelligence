FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY secrets_helper.py .
COPY msk_producer.py .
COPY game_events_producer/main.py ./game_events_producer/main.py

CMD ["python", "game_events_producer/main.py"]
